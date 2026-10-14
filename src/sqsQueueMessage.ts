import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";

import type { QueueMessage } from "./worker.js";

const MAX_VISIBILITY_SECONDS = 12 * 60 * 60;

/** A single received SQS delivery, identified by its current receipt handle. */
export class SqsQueueMessage implements QueueMessage {
  constructor(
    private readonly client: SQSClient,
    private readonly queueUrl: string,
    readonly jobId: string,
    readonly receiveCount: number,
    private readonly receiptHandle: string,
  ) {}

  async ack(): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: this.receiptHandle,
      }),
    );
  }

  async defer(delayMs: number): Promise<void> {
    await this.changeVisibility(delayMs);
  }

  async extendVisibility(durationMs: number): Promise<void> {
    await this.changeVisibility(durationMs);
  }

  private async changeVisibility(delayMs: number): Promise<void> {
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      throw new Error("visibility duration must be a non-negative finite number");
    }
    const visibilitySeconds = Math.max(
      1,
      Math.min(Math.ceil(delayMs / 1_000), MAX_VISIBILITY_SECONDS),
    );
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: this.receiptHandle,
        VisibilityTimeout: visibilitySeconds,
      }),
    );
  }
}
