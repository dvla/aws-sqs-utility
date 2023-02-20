const MIN_VISIBILITY_TIMEOUT = 1;

function toDateString(timestamp) {
    return new Date(Number(timestamp)).toISOString();
}

function reduceMessageAttributes(attributes) {
    if (!attributes) {
        return undefined;
    }

    return Object.entries(attributes).reduce((messageAttributes, [key, attribute]) => {
        return {
            ...messageAttributes,
            [key]: {
                DataType: attribute.DataType,
                StringValue: attribute.StringValue,
                BinaryValue: attribute.BinaryValue,
            },
        };
    }, {});
}

function toCsvMessage(sqsMessage) {
    const csvMessage = {
        MessageId: sqsMessage.MessageId,
        SenderId: sqsMessage.Attributes.SenderId,
        Sent: toDateString(sqsMessage.Attributes.SentTimestamp),
        FirstReceived: toDateString(sqsMessage.Attributes.ApproximateFirstReceiveTimestamp),
        ReceiveCount: sqsMessage.Attributes.ApproximateReceiveCount,
        Body: sqsMessage.Body,
        MessageAttributes: reduceMessageAttributes(sqsMessage.MessageAttributes),
        ReceiptHandle: sqsMessage.ReceiptHandle,
    };

    // FIFO
    if (sqsMessage.Attributes.MessageGroupId != null) {
        csvMessage.MessageGroupId = sqsMessage.Attributes.MessageGroupId;
        csvMessage.MessageDeduplicationId = sqsMessage.Attributes.MessageDeduplicationId;
    }

    return csvMessage;
}

function toSqsSend(csvMessage) {
    const sqsMessage = {
        Id: csvMessage.MessageId,
        MessageBody: csvMessage.Body,
        MessageAttributes: csvMessage.MessageAttributes,
    };

    // FIFO
    if (csvMessage.MessageGroupId != null) {
        sqsMessage.MessageGroupId = csvMessage.MessageGroupId;
        sqsMessage.MessageDeduplicationId = csvMessage.MessageDeduplicationId;
    }

    return sqsMessage;
}

function toSqsDelete(csvMessage) {
    return {
        Id: csvMessage.MessageId,
        ReceiptHandle: csvMessage.ReceiptHandle,
    };
}

function toSqsChangeVisibilityTimeout(receiptHandle, visibility, i) {
    return {
        Id: `${i}`,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibility,
    };
}

module.exports = {
    MIN_VISIBILITY_TIMEOUT,

    sqsReceiveMessages: async (queue, sqs, options = {}) => {
        const { maxNumberOfMessages = 10, visibilityTimeout = 30, waitTime = 5 } = options;

        if (visibilityTimeout < MIN_VISIBILITY_TIMEOUT) {
            throw new Error('Visibility timeout too low');
        }

        const response = await sqs
            .receiveMessage({
                AttributeNames: ['All'],
                MaxNumberOfMessages: Math.min(maxNumberOfMessages, 10),
                MessageAttributeNames: ['All'],
                QueueUrl: queue,
                VisibilityTimeout: visibilityTimeout,
                WaitTimeSeconds: Math.min(visibilityTimeout - MIN_VISIBILITY_TIMEOUT, waitTime),
            })
            .promise();

        return (response.Messages || []).map(toCsvMessage);
    },

    sqsSendMessages: async (messages, queue, sqs) => {
        const entries = messages.map(toSqsSend);

        const response = await sqs
            .sendMessageBatch({
                QueueUrl: queue,
                Entries: entries,
            })
            .promise();

        // eslint-disable-next-line no-console
        response.Failed.map((failure) => console.error(`Failed to send ${failure.Id}`));

        return entries.length - response.Failed.length;
    },

    sqsDeleteMessages: async (messages, queue, sqs) => {
        const entries = messages.map(toSqsDelete);

        const response = await sqs
            .deleteMessageBatch({
                QueueUrl: queue,
                Entries: entries,
            })
            .promise();

        // eslint-disable-next-line no-console
        response.Failed.map((failure) => console.error(`Failed to delete ${failure.Id}`));

        return entries.length - response.Failed.length;
    },

    sqsChangeVisibilityTimeout: async (receiptHandles, queue, sqs, visibility = 0) => {
        const entries = receiptHandles.map((handle, i) => toSqsChangeVisibilityTimeout(handle, visibility, i));

        const response = await sqs
            .changeMessageVisibilityBatch({
                QueueUrl: queue,
                Entries: entries,
            })
            .promise();

        return entries.length - response.Failed.length;
    },

    listQueues: async (sqs) => {
        const data = await sqs.listQueues().promise();

        return data.QueueUrls;
    },

    describeQueue: async (queue, sqs) => {
        const data = await sqs
            .getQueueAttributes({
                QueueUrl: queue,
                AttributeNames: [
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesDelayed',
                    'ApproximateNumberOfMessagesNotVisible',
                ],
            })
            .promise();

        return data;
    },
};
