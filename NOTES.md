### Part 1

I noticed the following issues while reading the document:
1. Import and export share one work queue and one worker fleet.
Import and export process usually requires different computing resources. The file sizes are also very different. It is better to have separate queues and worker fleets/pools that fit imports and exports.
2. It appears that workers update S3 directory jobs/{jobId}/result directory without checking the prior state.
What if a RUNNING state arrives after the SUCCESS state?
3. How does the system handle onboard night?
Onboarding event has 3x throughput and much higher traffic. Queue and worker pool need to scale up accordingly without overwhelming running jobs.
4. The worker fleet scales based on queue depth.
Queue depth is an important factor for scaling. It is usually nice to include other factors like the age of the oldest message.
5. Worker run as 1 CPU and 4GB memory Fargate task with up to 10 message concurrently. Has the team estimated and compared the costs between different worker dimensions? Because multi-core CPU and higher memory may have overall better performance as wellas cost-effectiveness. Also, a single CPU with 4GB memory running 10 conversions sounds way too high.
6. Webhook
I would utilize outbox and publisher mechanism to guarentee atomic operation for any downstream service like webhook.
7. It says DynamoDB stores job metadata. Does it support safe message/task claim and state transitioning? Because the database should be the source of truth to manage the states. I would explicitly add the state machine mechanism to DynamoDB if it doesn't already provide that. For example, message visibility/worker lease/fencing token.


Then I asked AI (GPT-5.6 Sol) to read the document and identify any issues for PART 1. I wanted to see what AI can discover and if I missed any important area. Below is the response:
```
Worker sizing is unsafe: Ten 2-GB, single-threaded conversions cannot run safely on 1 vCPU/4 GB. Use one conversion per task and provision sufficient export disk for packages up to 40 GB.

Imports and exports need separate queues/fleets: Their duration, disk usage, timeout, retry, and scaling characteristics differ significantly.

Autoscaling needs better signals: Scale by backlog per task, alert on oldest-message age, and treat retries, OOMs, and DLQ messages as health signals.

Burst capacity must be calculated: Choose a backlog-clearing target and derive concurrency from processing times. Under the sensitivity assumptions, approximately 102 tasks are required.

Duplicate execution needs fencing: Use a conditional DynamoDB claim, authoritative lease, and attempt token. A worker that loses the claim must not ack its duplicate message.

Result publication must be safe: Write to attempt-specific S3 keys and conditionally publish only the winning key. Stale attempts must not overwrite results.

Retries need clear boundaries: Distinguish permanent input errors from transient failures, kill and reap timed-out converters, and use DynamoDB attempts—not SQS receive count—as the retry authority.

Every job must become terminal: A reconciler/DLQ consumer must handle expired leases, undispatched jobs, exhausted deliveries, pending webhooks, and stuck queued or running records.

Dual writes need recovery: Both DynamoDB-to-SQS submission and success-to-webhook dispatch can fail between operations. Use reconciliation or DynamoDB Streams/outbox state.

Deployments must protect active work: Use ECS task scale-in protection, drain old workers, run a capacity-limited canary, label metrics by version, and monitor permanent as well as retryable failures.

Costs need explicit assumptions: Result retention and external downloads are unspecified. S3 retention may cost about $8.4k/month, NAT processing over $12k/month, and internet egress may add substantially more. Use S3 lifecycle policies and a Gateway VPC Endpoint.

Keep v1 restrained: Retain DynamoDB and Standard SQS; avoid SQL, FIFO, Step Functions, and per-customer scheduling until measured thresholds justify them.
```

Alright. I am on the right direction, but missing areas in cost assumptions and boundaries of retries. AI also provided a lot of details in all these fields including the issues I already identified.

Then I double checked with another AI -- Claude to validate these issues, and got similar coverage.

Next, I wrote the structure of PART 1 design and prompted AI to work on the draft including supporting details and diagrams.

Then I read and validated the details of the draft; did some research on Fargate, SQS, and costs; and prompted AI to update the design for completeness, accuracy and readability.

### Part 2

I am not familiar with TypeScript and asked AI to go through the original Worker code and explain the logic. And I quickly scanned the usage of Promise in TypeScript.

Then I asked AI to analyze the issues in the project guide. It identified a bunch of issues, I validated and accepted below risks.

#### Correctness risks identified

- `get()` followed by unconditional `put()` lets duplicate deliveries start two
  conversions and lets an old attempt overwrite a newer state.
- Every attempt writes the same result key, so a stale converter can overwrite
  the winning artifact. Observation: “A slow attempt may finish after another attempt has already published a result.”
- `Promise.race()` stops waiting after 30 seconds but does not stop or reap the
  subprocess. Observation: “A timed-out subprocess may continue running unless its owner terminates and reaps it.”
- Invalid input (exit 2), resource termination (exit 137), timeout, and other
  infrastructure errors require different retry decisions.
- SQS receive count is not the same as conversion attempt count; duplicate
  deliveries may increment it without starting a converter.
- A failed state write or acknowledgement can cause redelivery or leave a job
  in an inaccurate state.
- A worker crash can leave `running` stuck without a durable lease and heartbeat.

#### Chosen repairs

I prioritized two related correctness issues:

1. **Single-owner publication.** `JobStore.claim()` atomically claims queued work
   with an attempt token. Success/failure transitions compare that token, and
   results use attempt-scoped keys. This prevents concurrent duplicate work in
   the normal claim path and prevents a late attempt from publishing over a
   newer one.
2. **Safe timeout and failure handling.** A timeout kills and reaps the converter
   before retry. Exit 2 fails immediately; timeout and exit 137 each retry once;
   other transient errors use the overall three-attempt limit. DynamoDB attempt
   count, rather than SQS receive count, is authoritative.

I requested AI to make changes on the interfaces because safe behavior cannot be implemented on top of an unconditional `put()`. `RunningConversion.completion` is also defined to settle only after the child has exited and been reaped, while `kill()` resolves only after cleanup.


#### Focused tests

Then I prompted AI to write tests that cover below scenarios:

- concurrent deliveries starting only one converter;
- a stale attempt finishing after a newer successful attempt;
- kill/reap occurring before timeout retry;
- a second timeout becoming terminal;
- timeout classification when the clock throws an ordinary `Error`;
- invalid input failing without retry;
- exit 137 retrying once and then becoming terminal;
- generic errors respecting the overall attempt limit; and
- failed converter cleanup becoming terminal.


#### Not fixed within the timebox

- The poll loop's ten-way concurrency and ECS task sizing; Part 1 changes this
  to one conversion per task.
- Durable lease expiry, heartbeats, and automatic crash recovery. The token
  fence is implemented, but the in-memory store does not run a lease clock.
- Real DynamoDB, SQS, S3, ECS task-protection, DLQ reconciler, or webhook
  adapters.
- Cancellation of the timeout timer after a fast successful conversion.
- Queue acknowledgement/retry API failures and the submission DynamoDB/SQS
  dual-write window.
- Import/export-specific result extensions and checksum/multipart validation.
- Metrics, structured logs, and cleanup of abandoned attempt objects.

If I continued, I would implement the DynamoDB conditional expressions and
lease heartbeat, then add adapter contract tests for acknowledgement
failures and expired-owner recovery.

## Follow-up branch notes

The original time-box notes above are preserved as the submission's historical
stopping point. This branch implements the next-step work described in the last
paragraph.

### Added implementation

- `JobStore.claim()` now accepts `nowMs`, lease duration, attempt token, and
  worker version. It atomically claims `queued` work or reclaims an expired
  `running` lease and increments the application attempt count.
- `renewLease()` succeeds only for the current token while its lease is live.
  The worker runs a periodic heartbeat, then extends SQS visibility after the
  DynamoDB renewal. If it can no longer prove ownership, it kills/reaps the
  converter and defers the message.
- Success and failure publication require `running` state, a matching fencing
  token, and an unexpired lease.
- `QueueMessage.retry()` is replaced by `defer(delayMs)`. Busy deliveries defer
  near lease expiry; conversion retries use bounded exponential backoff. The
  SQS adapter uses `DeleteMessage` for `ack()` and
  `ChangeMessageVisibility` for defer and visibility heartbeat operations.
- `DynamoDbJobStore` includes concrete conditional expressions for claim,
  heartbeat, success, and failure. After a failed claim, a strongly consistent
  read classifies the record as missing, terminal, or busy and returns the
  remaining lease delay.

### Added and corrected tests

- The stale-publication test now completes the old attempt while the newer
  token still owns a `running` job. This closes the discovered gap where all
  previous tests remained green after removing the token comparison.
- Tests cover expired-lease reclaim, rejection of the old owner's renew and
  publish calls, heartbeat renewal, and kill/defer after ownership loss.
- A worker-level acknowledgement-failure test proves that redelivery observes
  the terminal state and does not run a second conversion.
- Adapter tests cover failed `DeleteMessage`, visibility-delay rounding,
  queued/expired claim conditions, busy classification, and the token/live-
  lease conditions for heartbeat and publication.

### Remaining follow-up risks

- S3 upload/checksum/multipart cleanup, ECS task protection, the DLQ/reconciler,
  submission outbox, and webhook delivery are still design-only.
- `conversion.kill()` still needs a bounded graceful-terminate/SIGKILL deadline
  for the case where cleanup itself never settles.
- DynamoDB and SQS heartbeat operations are not atomic. DynamoDB remains the
  ownership authority; a visibility failure currently stops work
  conservatively and recovery waits for the renewed lease to expire.
- Lease timestamps use the client clock. Production needs synchronized hosts,
  lease duration well above expected skew/jitter, and a reclaim margin.
- Adapter tests use scripted SDK clients. DynamoDB Local/LocalStack tests should
  execute the expressions against an engine before deployment.
- The successful-conversion path does not cancel its outstanding timeout timer.
- Submission DynamoDB/SQS dual-write recovery, repeated queue API failures,
  metrics/structured logs, and abandoned-attempt cleanup remain unimplemented.
