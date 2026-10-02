import type {
  ClaimResult,
  FailureTransition,
  Job,
  JobStore,
} from "./worker.js";

/** Reference implementation of the conditional-write semantics used in tests. */
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

  async claim(id: string, attemptToken: string): Promise<ClaimResult> {
    const current = this.jobs.get(id);
    if (!current) return { kind: "missing" };
    if (current.status === "succeeded" || current.status === "failed") {
      return { kind: "terminal" };
    }
    if (current.status === "running") return { kind: "busy" };

    const claimed: Job = {
      ...current,
      status: "running",
      attempt: current.attempt + 1,
      activeAttemptToken: attemptToken,
    };
    this.jobs.set(id, claimed);
    return { kind: "claimed", job: clone(claimed) };
  }

  async finishSuccess(
    id: string,
    attemptToken: string,
    outputKey: string,
  ): Promise<boolean> {
    const current = this.ownedAttempt(id, attemptToken);
    if (!current) return false;

    const { activeAttemptToken: _discarded, error: _oldError, ...rest } = current;
    this.jobs.set(id, { ...rest, status: "succeeded", outputKey });
    return true;
  }

  async finishFailure(
    id: string,
    attemptToken: string,
    transition: FailureTransition,
  ): Promise<boolean> {
    const current = this.ownedAttempt(id, attemptToken);
    if (!current) return false;

    const { activeAttemptToken: _discarded, outputKey: _oldOutput, ...rest } =
      current;
    this.jobs.set(id, {
      ...rest,
      status: transition.status,
      error: transition.error,
      timeoutCount: transition.timeoutCount,
      resourceExhaustionCount: transition.resourceExhaustionCount,
    });
    return true;
  }

  private ownedAttempt(id: string, attemptToken: string): Job | undefined {
    const job = this.jobs.get(id);
    if (
      !job ||
      job.status !== "running" ||
      job.activeAttemptToken !== attemptToken
    ) {
      return undefined;
    }
    return job;
  }
}

function clone(job: Job): Job {
  return { ...job };
}
