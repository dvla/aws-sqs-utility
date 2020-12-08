/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */

const {
    sqsReceiveMessages,
    sqsDeleteMessages,
    sqsChangeVisibilityTimeout,
    MIN_VISIBILITY_TIMEOUT,
} = require('./sqsClientUtils');

const DEFAULT_LIMIT = 1000;
const DEFAULT_TIMEOUT = 30;

class SqsReceive {
    constructor(sqs, { messageProcessor, limit, timeout } = {}) {
        this._sqs = sqs;
        this._messageProcessor = messageProcessor || ((message) => message);

        this.limit = limit || DEFAULT_LIMIT;
        this.timeout = timeout;

        this.receiveCount = 0;
        this.filteredCount = 0;
        this.writeCount = 0;
        this.deleteCount = 0;
    }

    async receiveMessages(queue, writer, deleteFromQueue) {
        let rawMessages;
        const receiptHandles = [];
        const endtime = Math.floor(Date.now() / 1000 + (this.timeout || DEFAULT_TIMEOUT));

        do {
            const maxNumberOfMessages = this.limit - this.receiveCount;
            const visibilityTimeout = Math.floor(endtime - Date.now() / 1000);
            let deleted = 0;

            if (visibilityTimeout < MIN_VISIBILITY_TIMEOUT) {
                console.warn(`Warning: Timeout reached (${this.timeout} seconds)`);
                break;
            }

            // eslint-disable-next-line no-await-in-loop
            rawMessages = await sqsReceiveMessages(queue, this._sqs, {
                maxNumberOfMessages,
                visibilityTimeout,
            });
            this.receiveCount += rawMessages.length;

            const messages = rawMessages.map(this._messageProcessor).filter((message) => !!message);
            this.filteredCount += messages.length;

            if (messages.length) {
                writer(messages);
                this.writeCount += messages.length;

                if (deleteFromQueue) {
                    // eslint-disable-next-line no-await-in-loop
                    deleted = await sqsDeleteMessages(messages, queue, this._sqs);
                }
            }

            this.deleteCount += deleted;

            // record receipt handles if any messages in batch have not been deleted
            if (deleted < rawMessages.length) {
                receiptHandles.push(rawMessages.map((message) => message.ReceiptHandle));
            }
        } while (rawMessages.length && this.receiveCount < this.limit);

        // make non-deleted messages visible for other SQS clients
        await Promise.all(receiptHandles.map((handles) => sqsChangeVisibilityTimeout(handles, queue, this._sqs)));
    }
}

module.exports = SqsReceive;
