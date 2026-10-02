import assert from "node:assert/strict";
import { test } from "node:test";

import {
  handle,
  type Clock,
  type Converter,
  type Job,
  type JobStore,
  type QueueMessage,
} from "../../src/originalWorker.js";

test("desired behavior: simultaneous deliveries start one conversion", async () => {
  const store = new SimultaneousReadStore(queuedJob());
  const first = new FakeMessage("job-1");
  const second = new FakeMessage("job-1");
  let starts = 0;
  const converter: Converter = {
    start: () => {
      starts += 1;
      return { completion: Promise.resolve(), kill: async () => {} };
    },
  };

  await Promise.all([
    handle(first, store, converter, neverClock),
    handle(second, store, converter, neverClock),
  ]);

  // Fails for the starter: get()+put() lets both deliveries start conversion.
  assert.equal(starts, 1);
});

test("desired behavior: timeout kills the converter before retry", async () => {
  const store = new MemoryStore(queuedJob());
  const message = new FakeMessage("job-1");
  let killed = false;
  const converter: Converter = {
    start: () => ({
      completion: new Promise<void>(() => {}),
      kill: async () => {
        killed = true;
      },
    }),
  };
  const timeoutClock: Clock = {
    timeout: async () => {
      throw new Error("timeout");
    },
  };

  await handle(message, store, converter, timeoutClock);

  // Fails for the starter: Promise.race() does not terminate the subprocess.
  assert.equal(killed, true);
  assert.equal(message.retries, 1);
});

class MemoryStore implements JobStore {
  protected job: Job;

  constructor(job: Job) {
    this.job = { ...job };
  }

  async get(id: string): Promise<Job | undefined> {
    return id === this.job.id ? { ...this.job } : undefined;
  }

  async put(job: Job): Promise<void> {
    this.job = { ...job };
  }
}

/** Makes both starter handlers read the same queued state before either writes. */
class SimultaneousReadStore extends MemoryStore {
  private reads = 0;
  private readonly bothRead = deferred<void>();

  override async get(id: string): Promise<Job | undefined> {
    const snapshot = await super.get(id);
    this.reads += 1;
    if (this.reads === 2) this.bothRead.resolve();
    await this.bothRead.promise;
    return snapshot;
  }
}

class FakeMessage implements QueueMessage {
  readonly receiveCount = 1;
  acks = 0;
  retries = 0;

  constructor(readonly jobId: string) {}

  async ack(): Promise<void> {
    this.acks += 1;
  }

  async retry(): Promise<void> {
    this.retries += 1;
  }
}

const neverClock: Clock = {
  timeout: () => new Promise<never>(() => {}),
};

function queuedJob(): Job {
  return {
    id: "job-1",
    inputKey: "inputs/database.mdb",
    status: "queued",
    attempt: 0,
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}
