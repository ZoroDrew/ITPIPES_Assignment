import assert from "node:assert/strict";
import { test } from "node:test";

import { InMemoryJobStore } from "../src/inMemoryJobStore.js";
import {
  ConversionError,
  ConversionTimedOutError,
  handle,
  LeaseLostError,
  type Clock,
  type Converter,
  type QueueMessage,
  type RunningConversion,
  type StartLeaseHeartbeat,
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
  assert.equal(duplicate.defers, 1);
  assert.ok((duplicate.deferDelays[0] ?? 0) > 0);

  running.resolve();
  await ownerHandle;
  assert.equal(owner.acks, 1);
  assert.equal((await store.get("job-1"))?.status, "succeeded");
});

test("a stale attempt cannot publish while the newer owner is still running", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const oldCompletion = deferred<void>();
  const oldStarted = deferred<void>();
  const oldMessage = new FakeMessage("job-1");
  let nowMs = 1_000;
  const leaseOptions = {
    now: () => nowMs,
    leaseDurationMs: 1_000,
    heartbeatIntervalMs: 500,
  };

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
    { ...leaseOptions, tokenFactory: () => "old-token" },
  );
  await oldStarted.promise;

  // Reclaim after expiry, but keep the new owner running while the old one ends.
  nowMs = 2_001;
  const newMessage = new FakeMessage("job-1");
  const newCompletion = deferred<void>();
  const newStarted = deferred<void>();
  const newHandle = handle(
    newMessage,
    store,
    {
      start: () => {
        newStarted.resolve();
        return { completion: newCompletion.promise, kill: async () => {} };
      },
    },
    neverClock,
    { ...leaseOptions, tokenFactory: () => "new-token" },
  );
  await newStarted.promise;

  oldCompletion.resolve();
  await oldHandle;
  assert.equal((await store.get("job-1"))?.status, "running");
  assert.equal((await store.get("job-1"))?.activeAttemptToken, "new-token");

  newCompletion.resolve();
  await newHandle;

  const job = await store.get("job-1");
  assert.equal(job?.status, "succeeded");
  assert.equal(
    job?.outputKey,
    "jobs/job-1/attempts/new-token/result.json",
  );
  assert.equal(newMessage.acks, 1);
  assert.equal(oldMessage.acks, 0);
  assert.equal(oldMessage.defers, 1);
});

test("an expired lease is reclaimed once and the old owner is fenced", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const first = await store.claim({
    id: "job-1",
    attemptToken: "old-token",
    nowMs: 1_000,
    leaseDurationMs: 100,
    workerVersion: "v1",
  });
  assert.equal(first.kind, "claimed");

  const busy = await store.claim({
    id: "job-1",
    attemptToken: "too-early",
    nowMs: 1_050,
    leaseDurationMs: 100,
    workerVersion: "v2",
  });
  assert.deepEqual(busy, { kind: "busy", retryAfterMs: 50 });

  const recovered = await store.claim({
    id: "job-1",
    attemptToken: "new-token",
    nowMs: 1_101,
    leaseDurationMs: 100,
    workerVersion: "v2",
  });
  assert.equal(recovered.kind, "claimed");
  assert.equal(recovered.kind === "claimed" && recovered.job.attempt, 2);
  assert.equal(
    await store.renewLease({
      id: "job-1",
      attemptToken: "old-token",
      nowMs: 1_102,
      leaseDurationMs: 100,
    }),
    false,
  );
  assert.equal(
    await store.finishSuccess(
      "job-1",
      "old-token",
      "attempts/old/result.json",
      1_102,
    ),
    false,
  );
});

test("heartbeat renews a lease and ownership loss kills the converter", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  const heartbeat = new ManualHeartbeat();
  const started = deferred<void>();
  let nowMs = 1_000;
  let killed = false;
  const message = new FakeMessage("job-1");

  const ownerHandle = handle(
    message,
    store,
    {
      start: () => {
        started.resolve();
        return {
          completion: new Promise<void>(() => {}),
          kill: async () => {
            killed = true;
          },
        };
      },
    },
    neverClock,
    {
      now: () => nowMs,
      leaseDurationMs: 100,
      heartbeatIntervalMs: 50,
      tokenFactory: () => "owner-token",
      startHeartbeat: heartbeat.start,
    },
  );
  await started.promise;

  nowMs = 1_050;
  await heartbeat.pulse();
  assert.equal((await store.get("job-1"))?.leaseExpiresAtMs, 1_150);

  nowMs = 1_151;
  const reclaimed = await store.claim({
    id: "job-1",
    attemptToken: "new-token",
    nowMs,
    leaseDurationMs: 100,
    workerVersion: "v2",
  });
  assert.equal(reclaimed.kind, "claimed");

  await heartbeat.pulse();
  await ownerHandle;
  assert.equal(killed, true);
  assert.equal(message.acks, 0);
  assert.equal(message.defers, 1);
  assert.equal((await store.get("job-1"))?.activeAttemptToken, "new-token");
});

test("an acknowledgement failure redelivers without rerunning conversion", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  let starts = 0;
  const first = new FailingAckMessage("job-1");
  const converter: Converter = {
    start: () => {
      starts += 1;
      return { completion: Promise.resolve(), kill: async () => {} };
    },
  };

  await assert.rejects(
    handle(
      first,
      store,
      converter,
      neverClock,
      { tokenFactory: () => "published-before-ack-failure" },
    ),
    /DeleteMessage failed/,
  );
  assert.equal((await store.get("job-1"))?.status, "succeeded");

  const redelivery = new FakeMessage("job-1");
  await handle(redelivery, store, converter, neverClock, {
    tokenFactory: () => "must-not-be-used",
  });
  assert.equal(starts, 1);
  assert.equal(redelivery.acks, 1);
  assert.equal(redelivery.defers, 0);
});

test("timeout kills and reaps the converter before retrying", async () => {
  const store = new InMemoryJobStore([queuedJob()]);
  let killed = false;
  const message = new FakeMessage("job-1", () => {
    assert.equal(killed, true, "message deferred before converter cleanup");
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
  assert.equal(message.defers, 1);
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
  assert.equal(message.defers, 1);
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
  assert.equal(message.defers, 0);
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
  assert.equal(message.defers, 0);
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
  assert.equal(first.defers, 1);

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
  assert.equal(second.defers, 0);
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
  assert.equal(message.defers, 0);
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
  assert.equal(message.defers, 0);
});

class FakeMessage implements QueueMessage {
  readonly receiveCount = 1;
  acks = 0;
  defers = 0;
  readonly deferDelays: number[] = [];
  visibilityExtensions = 0;

  constructor(
    readonly jobId: string,
    private readonly beforeDefer?: (delayMs: number) => void,
  ) {}

  async ack(): Promise<void> {
    this.acks += 1;
  }

  async extendVisibility(_durationMs: number): Promise<void> {
    this.visibilityExtensions += 1;
  }

  async defer(delayMs: number): Promise<void> {
    this.beforeDefer?.(delayMs);
    this.deferDelays.push(delayMs);
    this.defers += 1;
  }
}

class FailingAckMessage extends FakeMessage {
  override async ack(): Promise<void> {
    throw new Error("DeleteMessage failed");
  }
}

class ManualHeartbeat {
  private renew: (() => Promise<boolean>) | undefined;
  private readonly lost = deferred<never>();

  readonly start: StartLeaseHeartbeat = (renew) => {
    this.renew = renew;
    return { lost: this.lost.promise, stop: async () => {} };
  };

  async pulse(): Promise<void> {
    if (this.renew === undefined) throw new Error("heartbeat was not started");
    try {
      if (!(await this.renew())) this.lost.reject(new LeaseLostError());
    } catch (error) {
      this.lost.reject(error);
    }
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
