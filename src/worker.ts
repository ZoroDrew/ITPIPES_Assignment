import { randomUUID } from "node:crypto";

export type JobStatus = "queued" | "running" | "succeeded" | "failed";

export interface Job {
  id: string;
  inputKey: string;
  status: JobStatus;
  attempt: number;
  activeAttemptToken?: string;
  timeoutCount?: number;
  resourceExhaustionCount?: number;
  outputKey?: string;
  error?: string;
}

export type ClaimResult =
  | { kind: "claimed"; job: Job }
  | { kind: "busy" }
  | { kind: "terminal" }
  | { kind: "missing" };

export interface FailureTransition {
  status: "queued" | "failed";
  error: string;
  timeoutCount: number;
  resourceExhaustionCount: number;
}

export interface JobStore {
  /** Atomically changes queued -> running, increments attempt, and installs token. */
  claim(id: string, attemptToken: string): Promise<ClaimResult>;

  /** Applies only while this token still owns the running attempt. */
  finishSuccess(
    id: string,
    attemptToken: string,
    outputKey: string,
  ): Promise<boolean>;

  /** Applies only while this token still owns the running attempt. */
  finishFailure(
    id: string,
    attemptToken: string,
    transition: FailureTransition,
  ): Promise<boolean>;
}

export interface QueueMessage {
  jobId: string;
  receiveCount: number;
  ack(): Promise<void>;
  /**
   * Defers this delivery using queue visibility/backoff; it must not be an
   * immediate nack. A production busy claim should defer until near the
   * current owner's lease expiry.
   */
  retry(): Promise<void>;
}

export interface RunningConversion {
  /** Resolves or rejects only after the converter has exited and been reaped. */
  completion: Promise<void>;
  /** Terminates the converter and resolves only after it has been reaped. */
  kill(): Promise<void>;
}

export interface Converter {
  start(inputKey: string, outputKey: string): RunningConversion;
}

export interface Clock {
  timeout(ms: number): Promise<never>;
}

export interface HandlerOptions {
  timeoutMs: number;
  maxAttempts: number;
  tokenFactory: () => string;
}

export class ConversionTimedOutError extends Error {
  constructor(timeoutMs: number) {
    super(`conversion timed out after ${timeoutMs}ms`);
    this.name = "ConversionTimedOutError";
  }
}

export class ConversionError extends Error {
  constructor(
    message: string,
    readonly exitCode?: number,
    readonly retryable = true,
  ) {
    super(message);
    this.name = "ConversionError";
  }
}

const DEFAULT_OPTIONS: HandlerOptions = {
  timeoutMs: 15 * 60_000,
  maxAttempts: 3,
  tokenFactory: randomUUID,
};

export async function handle(
  message: QueueMessage,
  store: JobStore,
  converter: Converter,
  clock: Clock,
  options: Partial<HandlerOptions> = {},
): Promise<void> {
  const settings = { ...DEFAULT_OPTIONS, ...options };
  const attemptToken = settings.tokenFactory();
  const claim = await store.claim(message.jobId, attemptToken);

  if (claim.kind === "missing" || claim.kind === "terminal") {
    await message.ack();
    return;
  }

  if (claim.kind === "busy") {
    // Do not delete a duplicate while another delivery owns the job. If that
    // owner crashes, a later delivery must remain available to recover it.
    await message.retry();
    return;
  }

  const job = claim.job;
  const outputKey = `jobs/${job.id}/attempts/${attemptToken}/result.json`;
  let conversion: RunningConversion | undefined;

  try {
    conversion = converter.start(job.inputKey, outputKey);
    await Promise.race([
      conversion.completion,
      timeoutSignal(clock, settings.timeoutMs),
    ]);
  } catch (caught) {
    let error = toError(caught);

    if (error instanceof ConversionTimedOutError && conversion) {
      try {
        // Retrying before this resolves could overlap two converters for one job.
        await conversion.kill();
      } catch (killFailure) {
        error = new ConversionError(
          `converter cleanup failed after timeout: ${toError(killFailure).message}`,
          undefined,
          false,
        );
      }
    }

    const transition = decideFailure(job, error, settings.maxAttempts);
    const updated = await store.finishFailure(
      job.id,
      attemptToken,
      transition,
    );

    if (!updated) {
      // Ownership changed while this attempt was running. Never publish or
      // delete the shared queue message on behalf of the newer owner.
      await message.retry();
      return;
    }

    if (transition.status === "failed") {
      await message.ack();
    } else {
      await message.retry();
    }
    return;
  }

  const published = await store.finishSuccess(job.id, attemptToken, outputKey);
  if (!published) {
    // A recovered/newer attempt owns the job, so this result remains an
    // unreferenced attempt object and is later removed by an S3 lifecycle rule.
    await message.retry();
    return;
  }

  await message.ack();
}

/** The timeout branch owns the classification; Clock's rejection value does not. */
async function timeoutSignal(clock: Clock, timeoutMs: number): Promise<never> {
  try {
    await clock.timeout(timeoutMs);
  } catch {
    throw new ConversionTimedOutError(timeoutMs);
  }

  // Clock promises never resolve by contract, but preserve timeout semantics
  // if an adapter violates that contract.
  throw new ConversionTimedOutError(timeoutMs);
}

function decideFailure(
  job: Job,
  error: Error,
  maxAttempts: number,
): FailureTransition {
  let timeoutCount = job.timeoutCount ?? 0;
  let resourceExhaustionCount = job.resourceExhaustionCount ?? 0;
  let retryable = true;

  if (error instanceof ConversionTimedOutError) {
    timeoutCount += 1;
    retryable = timeoutCount < 2;
  } else if (error instanceof ConversionError) {
    if (error.exitCode === 2 || !error.retryable) {
      retryable = false;
    } else if (error.exitCode === 137) {
      resourceExhaustionCount += 1;
      retryable = resourceExhaustionCount < 2;
    }
  }

  if (job.attempt >= maxAttempts) {
    retryable = false;
  }

  return {
    status: retryable ? "queued" : "failed",
    error: error.message,
    timeoutCount,
    resourceExhaustionCount,
  };
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
