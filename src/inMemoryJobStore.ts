import type {
  ClaimRequest,
  ClaimResult,
  FailureTransition,
  Job,
  JobStore,
  RenewLeaseRequest,
} from "./worker.js";

/** Reference implementation of the DynamoDB conditional-write semantics. */
export class InMemoryJobStore implements JobStore {
  private readonly jobs = new Map<string, Job>();

  constructor(seed: Job[] = []) {
    for (const job of seed) {
      this.jobs.set(job.id, clone(job));
    }
  }

  async get(id: string): Promise<Job | undefined> {
    const job = this.jobs.get(id);
    return job ? clone(job) : undefined;
  }

  async claim(request: ClaimRequest): Promise<ClaimResult> {
    const current = this.jobs.get(request.id);
    if (!current) return { kind: "missing" };
    if (current.status === "succeeded" || current.status === "failed") {
      return { kind: "terminal" };
    }

    if (
      current.status === "running" &&
      (current.leaseExpiresAtMs === undefined ||
        current.leaseExpiresAtMs > request.nowMs)
    ) {
      return {
        kind: "busy",
        retryAfterMs:
          current.leaseExpiresAtMs === undefined
            ? request.leaseDurationMs
            : current.leaseExpiresAtMs - request.nowMs,
      };
    }

    const claimed: Job = {
      ...current,
      status: "running",
      attempt: current.attempt + 1,
      activeAttemptToken: request.attemptToken,
      leaseExpiresAtMs: request.nowMs + request.leaseDurationMs,
      workerVersion: request.workerVersion,
    };
    delete claimed.outputKey;
    delete claimed.error;
    this.jobs.set(request.id, claimed);
    return { kind: "claimed", job: clone(claimed) };
  }

  async renewLease(request: RenewLeaseRequest): Promise<boolean> {
    const current = this.ownedLiveAttempt(
      request.id,
      request.attemptToken,
      request.nowMs,
    );
    if (!current) return false;

    this.jobs.set(request.id, {
      ...current,
      leaseExpiresAtMs: request.nowMs + request.leaseDurationMs,
    });
    return true;
  }

  async finishSuccess(
    id: string,
    attemptToken: string,
    outputKey: string,
    nowMs: number,
  ): Promise<boolean> {
    const current = this.ownedLiveAttempt(id, attemptToken, nowMs);
    if (!current) return false;

    const {
      activeAttemptToken: _token,
      leaseExpiresAtMs: _lease,
      error: _error,
      ...rest
    } = current;
    this.jobs.set(id, { ...rest, status: "succeeded", outputKey });
    return true;
  }

  async finishFailure(
    id: string,
    attemptToken: string,
    transition: FailureTransition,
    nowMs: number,
  ): Promise<boolean> {
    const current = this.ownedLiveAttempt(id, attemptToken, nowMs);
    if (!current) return false;

    const {
      activeAttemptToken: _token,
      leaseExpiresAtMs: _lease,
      outputKey: _output,
      ...rest
    } = current;
    this.jobs.set(id, {
      ...rest,
      status: transition.status,
      error: transition.error,
      timeoutCount: transition.timeoutCount,
      resourceExhaustionCount: transition.resourceExhaustionCount,
    });
    return true;
  }

  private ownedLiveAttempt(
    id: string,
    attemptToken: string,
    nowMs: number,
  ): Job | undefined {
    const job = this.jobs.get(id);
    if (
      !job ||
      job.status !== "running" ||
      job.activeAttemptToken !== attemptToken ||
      job.leaseExpiresAtMs === undefined ||
      job.leaseExpiresAtMs <= nowMs
    ) {
      return undefined;
    }
    return job;
  }
}

function clone(job: Job): Job {
  return { ...job };
}
