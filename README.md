# Legacy File Conversion Worker

This repository contains the design review and a focused repair of the simplified
per-message worker from the exercise. It does not require AWS credentials,
Docker, or the vendor JVM.

## Requirements

- Node.js 20 or newer
- npm 10 or newer

## Install and verify

```bash
npm ci
npm run typecheck
npm run test:original # expected to fail: demonstrates the starter's bugs
npm run test:fixed    # expected to pass: verifies the repairs
npm test
```

`npm test` is the green verification command and aliases `test:fixed`.
`test:original` intentionally exits nonzero: its two failing tests reproduce the
starter worker's duplicate-conversion and leaked-subprocess bugs.
`npm run typecheck` runs TypeScript in strict mode without emitting artifacts.

## Layout

- `DESIGN.md` — Part 1 design review
- `architecture.svg` — high-level architecture diagram used by the review
- `src/originalWorker.ts` — verbatim starter worker retained for comparison
- `src/worker.ts` — repaired per-message handler and interfaces
- `src/inMemoryJobStore.ts` — reference conditional-write implementation
- `src/dynamoDbJobStore.ts` — DynamoDB lease/fencing conditional expressions
- `src/sqsQueueMessage.ts` — SQS delete and visibility-change adapter
- `test/worker.test.ts` — concurrency, lease, fencing, timeout, and error tests
- `test/adapters.test.ts` — AWS adapter command/conditional contract tests
- `test/baseline/originalWorker.test.ts` — intentionally red baseline tests
- `NOTES.md` — assumptions, remaining risks, stopping point, and AI usage

## Part 2 repair scope

The implementation changes `JobStore` from unconditional `get`/`put` methods to
atomic, intention-revealing operations:

- `claim()` performs `queued/expired-running -> running`, increments the
  attempt, and installs a token, lease expiry, and worker version atomically.
- A heartbeat conditionally extends the DynamoDB lease and SQS visibility.
  Failure to prove ownership kills/reaps the converter before deferring.
- `finishSuccess()` and `finishFailure()` require a running state, matching
  attempt token, and unexpired lease.
- Each attempt writes to its own S3 key. The conditional successful transition
  publishes the winning key.
- A timed-out converter is killed and reaped before the message is retried.
- Timeout classification comes from the timeout race branch, not the concrete
  error thrown by the clock adapter.
- Invalid input fails immediately; timeout and exit 137 each retry once; other
  transient failures respect the overall attempt limit.
- SQS `ack()` maps to `DeleteMessage`; `defer(delayMs)` maps to
  `ChangeMessageVisibility`, so busy and retry paths cannot hot-loop.
- SQS receive count is deliberately not used as the conversion attempt count.

The AWS adapters are dependency-injected and their contract tests use scripted
SDK clients, so tests require no AWS credentials. Deployment infrastructure,
S3 publication, the reconciler/DLQ consumer, and webhook delivery remain outside
this worker-focused code.
