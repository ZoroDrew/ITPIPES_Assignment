import assert from "node:assert/strict";
import { test } from "node:test";

import { InMemoryJobStore } from "../src/inMemoryJobStore.js";
import {
  ConversionError,
  ConversionTimedOutError,
  handle,
  type Clock,
  type Converter,
  type QueueMessage,
  type RunningConversion,
} from "../src/worker.js";

test("two concurrent deliveries start only one conversion", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const running = deferred<void>();
  const started = deferred<void>();
  let starts = 0;
  const converter: Converter = {
    start: () => {
      starts += 1;
      started.resolve();
      return { completion: running.promise, kill: async () => {} };
    },
  };
  const owner = new FakeMessage("job-1");
  const duplicate = new FakeMessage("job-1");

  const ownerHandle = handle(owner, store, converter, neverClock, {
    tokenFactory: () => "owner",
  });
  await started.promise;
  await handle(duplicate, store, converter, neverClock, {
    tokenFactory: () => "duplicate",
  });

  assert.equal(starts, 1);
  assert.equal(duplicate.acks, 0);
  assert.equal(duplicate.retries, 1);

  running.resolve();
  await ownerHandle;
  assert.equal(owner.acks, 1);
  assert.equal((await store.get("job-1"))?.status, "succeeded");
});

test("a stale attempt cannot overwrite the newer attempt's result", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const oldCompletion = deferred<void>();
  const oldStarted = deferred<void>();
  const oldMessage = new FakeMessage("job-1");

  const oldHandle = handle(
    oldMessage,
    store,
    {
      start: () => {
        oldStarted.resolve();
        return { completion: oldCompletion.promise, kill: async () => {} };
      },
    },
    neverClock,
    { tokenFactory: () => "old-token" },
  );
  await oldStarted.promise;

  // Simulate lease recovery while the old converter is still alive.
  assert.equal(
    await store.finishFailure("job-1", "old-token", {
      status: "queued",
      error: "lease expired",
      timeoutCount: 0,
      resourceExhaustionCount: 0,
    }),
    true,
  );

  const newMessage = new FakeMessage("job-1");
  await handle(
    newMessage,
    store,
    immediateSuccessConverter,
    neverClock,
    { tokenFactory: () => "new-token" },
  );

  oldCompletion.resolve();
  await oldHandle;

  const job = await store.get("job-1");
  assert.equal(job?.status, "succeeded");
  assert.equal(
    job?.outputKey,
    "jobs/job-1/attempts/new-token/result.json",
  );
  assert.equal(newMessage.acks, 1);
  assert.equal(oldMessage.acks, 0);
  assert.equal(oldMessage.retries, 1);
});

test("timeout kills and reaps the converter before retrying", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  let killed = false;
  const message = new FakeMessage("job-1", () => {
    assert.equal(killed, true, "message retried before converter cleanup");
  });
  const converter: Converter = {
    start: () => ({
      completion: new Promise<void>(() => {}),
      kill: async () => {
        killed = true;
      },
    }),
  };

  await handle(message, store, converter, immediateTimeoutClock, {
    tokenFactory: () => "timed-out",
  });

  const job = await store.get("job-1");
  assert.equal(killed, true);
  assert.equal(message.retries, 1);
  assert.equal(job?.status, "queued");
  assert.equal(job?.timeoutCount, 1);
});

test("timeout classification does not depend on the clock error type", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const message = new FakeMessage("job-1");
  let killed = false;
  const genericErrorClock: Clock = {
    timeout: async () => {
      throw new Error("timer adapter expired");
    },
  };

  await handle(
    message,
    store,
    {
      start: () => ({
        completion: new Promise<void>(() => {}),
        kill: async () => {
          killed = true;
        },
      }),
    },
    genericErrorClock,
    { tokenFactory: () => "generic-clock-error" },
  );

  const job = await store.get("job-1");
  assert.equal(killed, true);
  assert.equal(job?.status, "queued");
  assert.equal(job?.timeoutCount, 1);
  assert.match(job?.error ?? "", /conversion timed out/);
  assert.equal(message.retries, 1);
});

test("a second timeout is terminal", async () => {
  const store = new InMemoryJobStore([
    { ...queuedJob(), attempt: 1, timeoutCount: 1 },
  ]);
  const message = new FakeMessage("job-1");

  await handle(
    message,
    store,
    {
      start: () => ({
        completion: new Promise<void>(() => {}),
        kill: async () => {},
      }),
    },
    immediateTimeoutClock,
    { tokenFactory: () => "second-timeout" },
  );

  const job = await store.get("job-1");
  assert.equal(job?.status, "failed");
  assert.equal(job?.timeoutCount, 2);
  assert.equal(message.acks, 1);
  assert.equal(message.retries, 0);
});

test("invalid input fails immediately without retry", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const message = new FakeMessage("job-1");

  await handle(
    message,
    store,
    rejectingConverter(new ConversionError("required table missing", 2)),
    neverClock,
    { tokenFactory: () => "invalid" },
  );

  assert.equal((await store.get("job-1"))?.status, "failed");
  assert.equal(message.acks, 1);
  assert.equal(message.retries, 0);
});

test("exit 137 retries once and the second occurrence fails", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const first = new FakeMessage("job-1");

  await handle(
    first,
    store,
    rejectingConverter(new ConversionError("killed", 137)),
    neverClock,
    { tokenFactory: () => "oom-1" },
  );
  assert.equal((await store.get("job-1"))?.status, "queued");
  assert.equal(first.retries, 1);

  const second = new FakeMessage("job-1");
  await handle(
    second,
    store,
    rejectingConverter(new ConversionError("killed again", 137)),
    neverClock,
    { tokenFactory: () => "oom-2" },
  );

  const job = await store.get("job-1");
  assert.equal(job?.status, "failed");
  assert.equal(job?.resourceExhaustionCount, 2);
  assert.equal(second.acks, 1);
  assert.equal(second.retries, 0);
});

test("generic transient errors stop at the overall attempt limit", async () => {
  const store = new InMemoryJobStore([{ ...queuedJob(), attempt: 2 }]);
  const message = new FakeMessage("job-1");

  await handle(
    message,
    store,
    rejectingConverter(new Error("temporary dependency failure")),
    neverClock,
    { maxAttempts: 3, tokenFactory: () => "last-attempt" },
  );

  const job = await store.get("job-1");
  assert.equal(job?.attempt, 3);
  assert.equal(job?.status, "failed");
  assert.equal(message.acks, 1);
  assert.equal(message.retries, 0);
});

test("failed timeout cleanup is terminal and is not retried", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const message = new FakeMessage("job-1");

  await handle(
    message,
    store,
    {
      start: () => ({
        completion: new Promise<void>(() => {}),
        kill: async () => {
          throw new Error("process did not exit");
        },
      }),
    },
    immediateTimeoutClock,
    { tokenFactory: () => "cleanup-failed" },
  );

  const job = await store.get("job-1");
  assert.equal(job?.status, "failed");
  assert.match(job?.error ?? "", /cleanup failed.*process did not exit/);
  assert.equal(message.acks, 1);
  assert.equal(message.retries, 0);
});

class FakeMessage implements QueueMessage {
  readonly receiveCount = 1;
  acks = 0;
  retries = 0;

  constructor(
    readonly jobId: string,
    private readonly beforeRetry?: () => void,
  ) {}

  async ack(): Promise<void> {
    this.acks += 1;
  }

  async retry(): Promise<void> {
    this.beforeRetry?.();
    this.retries += 1;
  }
}

const neverClock: Clock = {
  timeout: () => new Promise<never>(() => {}),
};

const immediateTimeoutClock: Clock = {
  timeout: async (ms) => {
    throw new ConversionTimedOutError(ms);
  },
};

const immediateSuccessConverter: Converter = {
  start: () => ({ completion: Promise.resolve(), kill: async () => {} }),
};

function rejectingConverter(error: Error): Converter {
  return {
    start: (): RunningConversion => ({
      completion: Promise.reject(error),
      kill: async () => {},
    }),
  };
}

function queuedJob() {
  return {
    id: "job-1",
    inputKey: "inputs/database.mdb",
    status: "queued" as const,
    attempt: 0,
    timeoutCount: 0,
    resourceExhaustionCount: 0,
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
