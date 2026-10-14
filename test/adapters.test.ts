import assert from "node:assert/strict";
import { test } from "node:test";

import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import {
  GetCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from "@aws-sdk/lib-dynamodb";

import { DynamoDbJobStore } from "../src/dynamoDbJobStore.js";
import { SqsQueueMessage } from "../src/sqsQueueMessage.js";

test("SQS ack rejects when DeleteMessage fails", async () => {
  const client = new ScriptedClient([
    () => {
      throw new Error("SQS unavailable");
    },
  ]);
  const message = new SqsQueueMessage(
    client as unknown as SQSClient,
    "https://sqs.example/jobs",
    "job-1",
    1,
    "receipt-1",
  );

  await assert.rejects(message.ack(), /SQS unavailable/);
  const command = client.commands[0];
  assert.ok(command instanceof DeleteMessageCommand);
  assert.equal(command.input.ReceiptHandle, "receipt-1");
});

test("SQS defer changes visibility with a rounded-up delay", async () => {
  const client = new ScriptedClient([() => ({})]);
  const message = new SqsQueueMessage(
    client as unknown as SQSClient,
    "https://sqs.example/jobs",
    "job-1",
    2,
    "receipt-2",
  );

  await message.defer(1_250);
  const command = client.commands[0];
  assert.ok(command instanceof ChangeMessageVisibilityCommand);
  assert.equal(command.input.VisibilityTimeout, 2);
  assert.equal(command.input.ReceiptHandle, "receipt-2");
});

test("DynamoDB claim condition accepts queued or expired running work", async () => {
  const client = new ScriptedClient([
    () => ({
      Attributes: {
        id: "job-1",
        inputKey: "inputs/database.mdb",
        status: "running",
        attempt: 2,
        activeAttemptToken: "token-b",
        leaseExpiresAtMs: 1_600,
        workerVersion: "v2",
      },
    }),
  ]);
  const store = new DynamoDbJobStore(
    client as unknown as DynamoDBDocumentClient,
    "jobs",
  );

  const result = await store.claim({
    id: "job-1",
    attemptToken: "token-b",
    nowMs: 1_000,
    leaseDurationMs: 600,
    workerVersion: "v2",
  });

  assert.equal(result.kind, "claimed");
  const command = client.commands[0];
  assert.ok(command instanceof UpdateCommand);
  assert.match(command.input.ConditionExpression ?? "", /#status = :queued/);
  assert.match(command.input.ConditionExpression ?? "", /#lease <= :now/);
  assert.equal(command.input.ExpressionAttributeValues?.[":token"], "token-b");
  assert.equal(command.input.ExpressionAttributeValues?.[":newLease"], 1_600);
  assert.equal(command.input.ReturnValues, "ALL_NEW");
});

test("DynamoDB failed claim strongly reads the owner and returns lease delay", async () => {
  const conditionalFailure = Object.assign(new Error("condition failed"), {
    name: "ConditionalCheckFailedException",
  });
  const client = new ScriptedClient([
    () => {
      throw conditionalFailure;
    },
    () => ({
      Item: {
        id: "job-1",
        inputKey: "inputs/database.mdb",
        status: "running",
        attempt: 1,
        activeAttemptToken: "token-a",
        leaseExpiresAtMs: 1_600,
      },
    }),
  ]);
  const store = new DynamoDbJobStore(
    client as unknown as DynamoDBDocumentClient,
    "jobs",
  );

  const result = await store.claim({
    id: "job-1",
    attemptToken: "token-b",
    nowMs: 1_000,
    leaseDurationMs: 600,
    workerVersion: "v2",
  });

  assert.deepEqual(result, { kind: "busy", retryAfterMs: 600 });
  assert.ok(client.commands[1] instanceof GetCommand);
  const get = client.commands[1] as GetCommand;
  assert.equal(get.input.ConsistentRead, true);
});

test("DynamoDB publication condition requires status, token, and live lease", async () => {
  const client = new ScriptedClient([() => ({})]);
  const store = new DynamoDbJobStore(
    client as unknown as DynamoDBDocumentClient,
    "jobs",
  );

  assert.equal(
    await store.finishSuccess(
      "job-1",
      "current-token",
      "jobs/job-1/attempts/current-token/result.json",
      1_000,
    ),
    true,
  );

  const command = client.commands[0];
  assert.ok(command instanceof UpdateCommand);
  assert.match(command.input.ConditionExpression ?? "", /#status = :running/);
  assert.match(command.input.ConditionExpression ?? "", /#token = :token/);
  assert.match(command.input.ConditionExpression ?? "", /#lease > :now/);
  assert.equal(
    command.input.ExpressionAttributeValues?.[":token"],
    "current-token",
  );
});

test("DynamoDB heartbeat renews only the current unexpired owner", async () => {
  const client = new ScriptedClient([() => ({})]);
  const store = new DynamoDbJobStore(
    client as unknown as DynamoDBDocumentClient,
    "jobs",
  );

  assert.equal(
    await store.renewLease({
      id: "job-1",
      attemptToken: "current-token",
      nowMs: 1_000,
      leaseDurationMs: 600,
    }),
    true,
  );

  const command = client.commands[0];
  assert.ok(command instanceof UpdateCommand);
  assert.match(command.input.ConditionExpression ?? "", /#status = :running/);
  assert.match(command.input.ConditionExpression ?? "", /#token = :token/);
  assert.match(command.input.ConditionExpression ?? "", /#lease > :now/);
  assert.equal(command.input.ExpressionAttributeValues?.[":newLease"], 1_600);
});

class ScriptedClient {
  readonly commands: unknown[] = [];

  constructor(private readonly steps: Array<() => unknown>) {}

  async send(command: unknown): Promise<unknown> {
    this.commands.push(command);
    const step = this.steps.shift();
    if (step === undefined) throw new Error("unexpected client command");
    return step();
  }
}
