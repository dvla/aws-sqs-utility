/* eslint no-underscore-dangle: ["error", { "allowAfterThis": true }] */

const { sqsSendMessages, sqsDeleteMessages } = require('./sqsClientUtils');

const SEND_BATCH_SIZE = 10;

function isEmptyMessage(message) {
    return !message.MessageId || !message.MessageId.trim();
}

function isInvalidMessage(message) {
    return !message.MessageId.match(/^[a-z0-9_-]{1,80}$/i);
}

function isInvalidMessageAttributes(message) {
    return (
        message.MessageAttributes === null ||
        (message.MessageAttributes && typeof message.MessageAttributes !== 'object')
    );
}

function logFailure(failure, readCount) {
    const rows =
        failure.batchStart >= 0 ? `row batch ${failure.batchStart}-${failure.batchEnd}` : `row ${readCount + 1}`;

    // eslint-disable-next-line no-console
    console.error(`Error (${rows}): ${failure.message || 'Unknown'}`);
}

class SqsModify {
    constructor(sqs, { messageProcessor = (message) => message } = {}) {
        this._sqs = sqs;
        this._messageProcessor = messageProcessor;
        this._promises = [];
        this._awaitingPromises = false;

        this.readCount = 0;
        this.filteredCount = 0;
        this.modifiedCount = 0;
    }

    awaitPromises(error) {
        if (!this._resolve || !this._reject) {
            throw new Error('awaitPromises() invoked too early');
        }

        if (this._awaitingPromises) {
            return;
        }

        this._awaitingPromises = true;

        Promise.allSettled(this._promises).then((results) => {
            const rejected = results.filter((result) => result.status === 'rejected');
            rejected.forEach((reject) => logFailure(reject.reason, this.readCount));

            if (error && rejected.length === 0) {
                logFailure(error, this.readCount);
            }

            if (error) {
                this._reject(`Aborted: ${error}`);
            } else {
                this._resolve();
            }
        });
    }

    modifyMessages(queue, reader, deleteFromQueue) {
        const sqsModifyMessages = deleteFromQueue ? sqsDeleteMessages : sqsSendMessages;

        const modifyMessageBatch = (batch, batchEnd) =>
            sqsModifyMessages(batch, queue, this._sqs)
                .then((count) => {
                    this.modifiedCount += count;
                })
                .catch((error) => {
                    reader.destroy(error);

                    // eslint-disable-next-line no-param-reassign
                    error.batchStart = batchEnd - batch.length + 1;
                    // eslint-disable-next-line no-param-reassign
                    error.batchEnd = batchEnd;

                    throw error;
                });

        return new Promise((resolve, reject) => {
            this._resolve = resolve;
            this._reject = reject;

            let batch = [];

            reader
                .on('error', (error) => {
                    this.awaitPromises(error);
                })
                .on('data', (rawMessage) => {
                    this.readCount += 1;
                    const message = this._messageProcessor(rawMessage);

                    if (!message) {
                        return;
                    }

                    this.filteredCount += 1;

                    if (isEmptyMessage(message)) {
                        // eslint-disable-next-line no-console
                        console.error(`Ignoring empty message (row ${this.readCount})`);
                        return;
                    }

                    if (isInvalidMessage(message)) {
                        // eslint-disable-next-line no-console
                        console.error(`Ignoring invalid message (row ${this.readCount})`);
                        return;
                    }

                    if (isInvalidMessageAttributes(message)) {
                        // eslint-disable-next-line no-console
                        console.error(`Ignoring message due to invalid message attributes (row ${this.readCount})`);
                        return;
                    }

                    batch.push(message);

                    if (batch.length === SEND_BATCH_SIZE) {
                        this._promises.push(modifyMessageBatch(batch, this.readCount));
                        batch = [];
                    }
                })
                .on('end', () => {
                    if (batch.length) {
                        this._promises.push(modifyMessageBatch(batch, this.readCount));
                        batch = [];
                    }

                    this.awaitPromises();
                });
        });
    }
}

module.exports = SqsModify;
