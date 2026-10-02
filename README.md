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
- `test/worker.test.ts` — focused concurrency, fencing, timeout, and error tests
- `test/baseline/originalWorker.test.ts` — intentionally red baseline tests
- `NOTES.md` — assumptions, remaining risks, stopping point, and AI usage

## Part 2 repair scope

The implementation changes `JobStore` from unconditional `get`/`put` methods to
atomic, intention-revealing operations:

- `claim()` performs `queued -> running`, increments the attempt, and installs
  an attempt token atomically.
- `finishSuccess()` and `finishFailure()` succeed only for the token that still
  owns the running attempt.
- Each attempt writes to its own S3 key. The conditional successful transition
  publishes the winning key.
- A timed-out converter is killed and reaped before the message is retried.
- Timeout classification comes from the timeout race branch, not the concrete
  error thrown by the clock adapter.
- Invalid input fails immediately; timeout and exit 137 each retry once; other
  transient failures respect the overall attempt limit.
- SQS receive count is deliberately not used as the conversion attempt count.

The in-memory store documents the conditions a DynamoDB adapter would implement
with conditional expressions. Production AWS adapters and infrastructure are
outside this exercise's code scope.
