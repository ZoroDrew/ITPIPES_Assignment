import {
  GetCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from "@aws-sdk/lib-dynamodb";

import type {
  ClaimRequest,
  ClaimResult,
  FailureTransition,
  Job,
  JobStatus,
  JobStore,
  RenewLeaseRequest,
} from "./worker.js";

/** DynamoDB implementation of the lease and fencing conditions used by workers. */
export class DynamoDbJobStore implements JobStore {
  constructor(
    private readonly client: DynamoDBDocumentClient,
    private readonly tableName: string,
  ) {}

  async claim(request: ClaimRequest): Promise<ClaimResult> {
    try {
      const response = await this.client.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { id: request.id },
          ConditionExpression:
            "attribute_exists(#id) AND (#status = :queued OR (#status = :running AND #lease <= :now))",
          UpdateExpression:
            "SET #status = :running, #attempt = #attempt + :one, #token = :token, #lease = :newLease, #workerVersion = :workerVersion REMOVE #outputKey, #error",
          ExpressionAttributeNames: {
            "#id": "id",
            "#status": "status",
            "#attempt": "attempt",
            "#token": "activeAttemptToken",
            "#lease": "leaseExpiresAtMs",
            "#workerVersion": "workerVersion",
            "#outputKey": "outputKey",
            "#error": "error",
          },
          ExpressionAttributeValues: {
            ":queued": "queued",
            ":running": "running",
            ":one": 1,
            ":token": request.attemptToken,
            ":now": request.nowMs,
            ":newLease": request.nowMs + request.leaseDurationMs,
            ":workerVersion": request.workerVersion,
          },
          ReturnValues: "ALL_NEW",
        }),
      );
      if (response.Attributes === undefined) {
        throw new Error("DynamoDB claim returned no attributes");
      }
      return { kind: "claimed", job: toJob(response.Attributes) };
    } catch (error) {
      if (!isConditionalCheckFailure(error)) throw error;
      return this.classifyFailedClaim(request);
    }
  }

  async renewLease(request: RenewLeaseRequest): Promise<boolean> {
    return this.conditionalUpdate(
      new UpdateCommand({
        TableName: this.tableName,
        Key: { id: request.id },
        ConditionExpression:
          "#status = :running AND #token = :token AND #lease > :now",
        UpdateExpression: "SET #lease = :newLease",
        ExpressionAttributeNames: {
          "#status": "status",
          "#token": "activeAttemptToken",
          "#lease": "leaseExpiresAtMs",
        },
        ExpressionAttributeValues: {
          ":running": "running",
          ":token": request.attemptToken,
          ":now": request.nowMs,
          ":newLease": request.nowMs + request.leaseDurationMs,
        },
      }),
    );
  }

  async finishSuccess(
    id: string,
    attemptToken: string,
    outputKey: string,
    nowMs: number,
  ): Promise<boolean> {
    return this.conditionalUpdate(
      new UpdateCommand({
        TableName: this.tableName,
        Key: { id },
        ConditionExpression:
          "#status = :running AND #token = :token AND #lease > :now",
        UpdateExpression:
          "SET #status = :succeeded, #outputKey = :outputKey REMOVE #token, #lease, #error",
        ExpressionAttributeNames: {
          "#status": "status",
          "#token": "activeAttemptToken",
          "#lease": "leaseExpiresAtMs",
          "#outputKey": "outputKey",
          "#error": "error",
        },
        ExpressionAttributeValues: {
          ":running": "running",
          ":succeeded": "succeeded",
          ":token": attemptToken,
          ":now": nowMs,
          ":outputKey": outputKey,
        },
      }),
    );
  }

  async finishFailure(
    id: string,
    attemptToken: string,
    transition: FailureTransition,
    nowMs: number,
  ): Promise<boolean> {
    return this.conditionalUpdate(
      new UpdateCommand({
        TableName: this.tableName,
        Key: { id },
        ConditionExpression:
          "#status = :running AND #token = :token AND #lease > :now",
        UpdateExpression:
          "SET #status = :nextStatus, #error = :error, #timeoutCount = :timeoutCount, #resourceCount = :resourceCount REMOVE #token, #lease, #outputKey",
        ExpressionAttributeNames: {
          "#status": "status",
          "#token": "activeAttemptToken",
          "#lease": "leaseExpiresAtMs",
          "#error": "error",
          "#timeoutCount": "timeoutCount",
          "#resourceCount": "resourceExhaustionCount",
          "#outputKey": "outputKey",
        },
        ExpressionAttributeValues: {
          ":running": "running",
          ":nextStatus": transition.status,
          ":token": attemptToken,
          ":now": nowMs,
          ":error": transition.error,
          ":timeoutCount": transition.timeoutCount,
          ":resourceCount": transition.resourceExhaustionCount,
        },
      }),
    );
  }

  private async classifyFailedClaim(
    request: ClaimRequest,
  ): Promise<ClaimResult> {
    const response = await this.client.send(
      new GetCommand({
        TableName: this.tableName,
        Key: { id: request.id },
        ConsistentRead: true,
      }),
    );
    if (response.Item === undefined) return { kind: "missing" };

    const job = toJob(response.Item);
    if (job.status === "succeeded" || job.status === "failed") {
      return { kind: "terminal" };
    }

    return {
      kind: "busy",
      retryAfterMs:
        job.leaseExpiresAtMs === undefined
          ? request.leaseDurationMs
          : Math.max(0, job.leaseExpiresAtMs - request.nowMs),
    };
  }

  private async conditionalUpdate(command: UpdateCommand): Promise<boolean> {
    try {
      await this.client.send(command);
      return true;
    } catch (error) {
      if (isConditionalCheckFailure(error)) return false;
      throw error;
    }
  }
}

function isConditionalCheckFailure(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "name" in error &&
    error.name === "ConditionalCheckFailedException"
  );
}

function toJob(item: Record<string, unknown>): Job {
  const status = item.status;
  if (!isJobStatus(status)) throw new Error("DynamoDB item has invalid status");
  if (typeof item.id !== "string" || typeof item.inputKey !== "string") {
    throw new Error("DynamoDB item is missing job identity fields");
  }
  if (typeof item.attempt !== "number") {
    throw new Error("DynamoDB item is missing attempt count");
  }

  const job: Job = {
    id: item.id,
    inputKey: item.inputKey,
    status,
    attempt: item.attempt,
  };
  copyString(item, job, "activeAttemptToken");
  copyNumber(item, job, "leaseExpiresAtMs");
  copyString(item, job, "workerVersion");
  copyNumber(item, job, "timeoutCount");
  copyNumber(item, job, "resourceExhaustionCount");
  copyString(item, job, "outputKey");
  copyString(item, job, "error");
  return job;
}

function isJobStatus(value: unknown): value is JobStatus {
  return (
    value === "queued" ||
    value === "running" ||
    value === "succeeded" ||
    value === "failed"
  );
}

function copyString(
  source: Record<string, unknown>,
  target: Job,
  key: "activeAttemptToken" | "workerVersion" | "outputKey" | "error",
): void {
  const value = source[key];
  if (typeof value === "string") target[key] = value;
}

function copyNumber(
  source: Record<string, unknown>,
  target: Job,
  key: "leaseExpiresAtMs" | "timeoutCount" | "resourceExhaustionCount",
): void {
  const value = source[key];
  if (typeof value === "number") target[key] = value;
}
