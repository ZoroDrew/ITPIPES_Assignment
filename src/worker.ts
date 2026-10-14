import { randomUUID } from "node:crypto";

export type JobStatus = "queued" | "running" | "succeeded" | "failed";

export interface Job {
  id: string;
  inputKey: string;
  status: JobStatus;
  attempt: number;
  activeAttemptToken?: string;
  leaseExpiresAtMs?: number;
  workerVersion?: string;
  timeoutCount?: number;
  resourceExhaustionCount?: number;
  outputKey?: string;
  error?: string;
}

export interface ClaimRequest {
  id: string;
  attemptToken: string;
  nowMs: number;
  leaseDurationMs: number;
  workerVersion: string;
}

export interface RenewLeaseRequest {
  id: string;
  attemptToken: string;
  nowMs: number;
  leaseDurationMs: number;
}

export type ClaimResult =
  | { kind: "claimed"; job: Job }
  | { kind: "busy"; retryAfterMs: number }
  | { kind: "terminal" }
  | { kind: "missing" };

export interface FailureTransition {
  status: "queued" | "failed";
  error: string;
  timeoutCount: number;
  resourceExhaustionCount: number;
}

export interface JobStore {
  /** Atomically claims queued work or an expired running lease. */
  claim(request: ClaimRequest): Promise<ClaimResult>;

  /** Extends only a live lease still owned by this token. */
  renewLease(request: RenewLeaseRequest): Promise<boolean>;

  /** Applies only while this token still owns a live running lease. */
  finishSuccess(
    id: string,
    attemptToken: string,
    outputKey: string,
    nowMs: number,
  ): Promise<boolean>;

  /** Applies only while this token still owns a live running lease. */
  finishFailure(
    id: string,
    attemptToken: string,
    transition: FailureTransition,
    nowMs: number,
  ): Promise<boolean>;
}

export interface QueueMessage {
  jobId: string;
  receiveCount: number;
  /** Deletes the current SQS delivery using its receipt handle. */
  ack(): Promise<void>;
  /** Extends the current in-flight delivery while its owner is healthy. */
  extendVisibility(durationMs: number): Promise<void>;
  /**
   * Keeps this delivery but makes it visible again after a bounded delay.
   * A production SQS adapter implements this with ChangeMessageVisibility.
   */
  defer(delayMs: number): Promise<void>;
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

export interface RunningLeaseHeartbeat {
  /** Rejects if renewal fails or this attempt no longer owns a live lease. */
  lost: Promise<never>;
  stop(): Promise<void>;
}

export type StartLeaseHeartbeat = (
  renew: () => Promise<boolean>,
  intervalMs: number,
) => RunningLeaseHeartbeat;

export interface HandlerOptions {
  timeoutMs: number;
  maxAttempts: number;
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  messageVisibilityMs: number;
  busyDeferBufferMs: number;
  ownershipLostDeferMs: number;
  workerVersion: string;
  now: () => number;
  retryDelayMs: (attempt: number) => number;
  tokenFactory: () => string;
  startHeartbeat: StartLeaseHeartbeat;
}

export class ConversionTimedOutError extends Error {
  constructor(timeoutMs: number) {
    super(`conversion timed out after ${timeoutMs}ms`);
    this.name = "ConversionTimedOutError";
  }
}

export class LeaseLostError extends Error {
  constructor(message = "job lease could not be renewed") {
    super(message);
    this.name = "LeaseLostError";
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

const MAX_SQS_VISIBILITY_MS = 12 * 60 * 60_000;

const DEFAULT_OPTIONS: HandlerOptions = {
  timeoutMs: 15 * 60_000,
  maxAttempts: 3,
  leaseDurationMs: 3 * 60_000,
  heartbeatIntervalMs: 60_000,
  messageVisibilityMs: 5 * 60_000,
  busyDeferBufferMs: 1_000,
  ownershipLostDeferMs: 30_000,
  workerVersion: process.env.WORKER_VERSION ?? "dev",
  now: Date.now,
  retryDelayMs: (attempt) => Math.min(5_000 * 2 ** (attempt - 1), 5 * 60_000),
  tokenFactory: randomUUID,
  startHeartbeat: startIntervalLeaseHeartbeat,
};

export async function handle(
  message: QueueMessage,
  store: JobStore,
  converter: Converter,
  clock: Clock,
  options: Partial<HandlerOptions> = {},
): Promise<void> {
  const settings = { ...DEFAULT_OPTIONS, ...options };
  validateLeaseSettings(settings);

  const attemptToken = settings.tokenFactory();
  const claim = await store.claim({
    id: message.jobId,
    attemptToken,
    nowMs: settings.now(),
    leaseDurationMs: settings.leaseDurationMs,
    workerVersion: settings.workerVersion,
  });

  if (claim.kind === "missing" || claim.kind === "terminal") {
    await message.ack();
    return;
  }

  if (claim.kind === "busy") {
    // Keep the duplicate as a recovery trigger, but avoid a hot receive loop.
    await message.defer(
      boundedDelay(claim.retryAfterMs + settings.busyDeferBufferMs),
    );
    return;
  }

  const job = claim.job;
  const heartbeat = settings.startHeartbeat(
    async () => {
      const renewed = await store.renewLease({
        id: job.id,
        attemptToken,
        nowMs: settings.now(),
        leaseDurationMs: settings.leaseDurationMs,
      });
      if (!renewed) return false;
      // SQS visibility is a delivery aid; DynamoDB remains ownership authority.
      await message.extendVisibility(settings.messageVisibilityMs);
      return true;
    },
    settings.heartbeatIntervalMs,
  );

  try {
    await runClaimedAttempt(
      message,
      store,
      converter,
      clock,
      settings,
      heartbeat,
      job,
      attemptToken,
    );
  } finally {
    await heartbeat.stop();
  }
}

async function runClaimedAttempt(
  message: QueueMessage,
  store: JobStore,
  converter: Converter,
  clock: Clock,
  settings: HandlerOptions,
  heartbeat: RunningLeaseHeartbeat,
  job: Job,
  attemptToken: string,
): Promise<void> {
  const outputKey = `jobs/${job.id}/attempts/${attemptToken}/result.json`;
  let conversion: RunningConversion | undefined;

  try {
    conversion = converter.start(job.inputKey, outputKey);
    await Promise.race([
      conversion.completion,
      timeoutSignal(clock, settings.timeoutMs),
      heartbeat.lost,
    ]);
  } catch (caught) {
    let error = toError(caught);

    if (error instanceof LeaseLostError) {
      if (conversion !== undefined) {
        // A worker that cannot prove ownership must stop producing immediately.
        await conversion.kill();
      }
      await message.defer(boundedDelay(settings.ownershipLostDeferMs));
      return;
    }

    if (error instanceof ConversionTimedOutError && conversion !== undefined) {
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
      settings.now(),
    );

    if (!updated) {
      // Ownership changed or the lease expired. Never mutate or ack for its owner.
      await message.defer(boundedDelay(settings.ownershipLostDeferMs));
      return;
    }

    if (transition.status === "failed") {
      await message.ack();
    } else {
      await message.defer(boundedDelay(settings.retryDelayMs(job.attempt)));
    }
    return;
  }

  const published = await store.finishSuccess(
    job.id,
    attemptToken,
    outputKey,
    settings.now(),
  );
  if (!published) {
    // This attempt's object is now an orphan; lifecycle cleanup removes it.
    await message.defer(boundedDelay(settings.ownershipLostDeferMs));
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

  // Preserve timeout semantics even if a Clock adapter violates its contract.
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

/** Default production heartbeat loop; tests can inject a manual scheduler. */
export function startIntervalLeaseHeartbeat(
  renew: () => Promise<boolean>,
  intervalMs: number,
): RunningLeaseHeartbeat {
  let stopped = false;
  let timer: NodeJS.Timeout | undefined;
  let inFlight = Promise.resolve();
  let rejectLost!: (reason: unknown) => void;
  const lost = new Promise<never>((_resolve, reject) => {
    rejectLost = reject;
  });

  const schedule = () => {
    timer = setTimeout(() => {
      if (stopped) return;
      inFlight = (async () => {
        try {
          if (!(await renew())) {
            throw new LeaseLostError();
          }
          if (!stopped) schedule();
        } catch (error) {
          if (!stopped) {
            rejectLost(
              error instanceof LeaseLostError
                ? error
                : new LeaseLostError(`lease renewal failed: ${toError(error).message}`),
            );
          }
        }
      })();
    }, intervalMs);
    timer.unref?.();
  };

  schedule();
  return {
    lost,
    stop: async () => {
      stopped = true;
      if (timer !== undefined) clearTimeout(timer);
      await inFlight;
    },
  };
}

function validateLeaseSettings(settings: HandlerOptions): void {
  if (settings.heartbeatIntervalMs <= 0) {
    throw new Error("heartbeatIntervalMs must be positive");
  }
  if (settings.leaseDurationMs <= settings.heartbeatIntervalMs) {
    throw new Error("leaseDurationMs must exceed heartbeatIntervalMs");
  }
  if (settings.messageVisibilityMs <= settings.heartbeatIntervalMs) {
    throw new Error("messageVisibilityMs must exceed heartbeatIntervalMs");
  }
}

function boundedDelay(delayMs: number): number {
  return Math.max(1_000, Math.min(Math.ceil(delayMs), MAX_SQS_VISIBILITY_MS));
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
