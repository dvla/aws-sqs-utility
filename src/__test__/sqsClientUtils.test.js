const {
    sqsReceiveMessages,
    sqsSendMessages,
    sqsDeleteMessages,
    sqsChangeVisibilityTimeout,
    listQueues,
    describeQueue,
} = require('../sqsClientUtils');

beforeEach(() => {
    jest.clearAllMocks();
    global.console.error = jest.fn();
    // global.console.error = jest.fn(global.console.error);
});

describe('sqsReceiveMessages', () => {
    it('should receive messages from queue', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Messages: [
                            {
                                MessageId: 'messageId1',
                                Attributes: {
                                    SenderId: 'senderId1',
                                    SentTimestamp: '1600961099012',
                                    ApproximateFirstReceiveTimestamp: '1600963774776',
                                    ApproximateReceiveCount: '5',
                                },
                                Body: 'body1',
                                ReceiptHandle: 'receipthandle1',
                            },
                            {
                                MessageId: 'messageId2',
                                Attributes: {
                                    SenderId: 'senderId2',
                                    SentTimestamp: '1600961099012',
                                    ApproximateFirstReceiveTimestamp: '1600963774776',
                                    ApproximateReceiveCount: '5',
                                },
                                MessageAttributes: {
                                    attribute1: {
                                        DataType: 'String',
                                        StringValue: 'string value',
                                    },
                                    attribute2: {
                                        DataType: 'Binary',
                                        BinaryValue: 'binaryvalue',
                                    },
                                },
                                Body: 'body2',
                                ReceiptHandle: 'receipthandle2',
                            },
                        ],
                    }),
            })),
        };

        // When
        const messages = await sqsReceiveMessages('test-queue', sqs);

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledWith({
            AttributeNames: ['All'],
            MaxNumberOfMessages: 10,
            MessageAttributeNames: ['All'],
            QueueUrl: 'test-queue',
            VisibilityTimeout: 30,
            WaitTimeSeconds: 5,
        });

        expect(messages[0]).toEqual({
            MessageId: 'messageId1',
            SenderId: 'senderId1',
            Sent: '2020-09-24T15:24:59.012Z',
            FirstReceived: '2020-09-24T16:09:34.776Z',
            ReceiveCount: '5',
            Body: 'body1',
            MessageAttributes: undefined,
            ReceiptHandle: 'receipthandle1',
        });
        expect(messages[1]).toEqual({
            MessageId: 'messageId2',
            SenderId: 'senderId2',
            Sent: '2020-09-24T15:24:59.012Z',
            FirstReceived: '2020-09-24T16:09:34.776Z',
            ReceiveCount: '5',
            Body: 'body2',
            MessageAttributes: {
                attribute1: {
                    DataType: 'String',
                    StringValue: 'string value',
                    BinaryValue: undefined,
                },
                attribute2: {
                    DataType: 'Binary',
                    StringValue: undefined,
                    BinaryValue: 'binaryvalue',
                },
            },
            ReceiptHandle: 'receipthandle2',
        });
    });

    it('should receive empty response from queue', async () => {
        // Given
        const sqs = {
            receiveMessage: () => ({
                promise: () => Promise.resolve({}),
            }),
        };

        // When
        const messages = await sqsReceiveMessages('test-queue', sqs);

        // Then
        expect(messages.length).toEqual(0);
    });

    it('should configure maxNumberOfMessages', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({
                promise: () => Promise.resolve({}),
            })),
        };

        // When
        await sqsReceiveMessages('test-queue', sqs, { maxNumberOfMessages: 5 });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledWith({
            AttributeNames: ['All'],
            MaxNumberOfMessages: 5,
            MessageAttributeNames: ['All'],
            QueueUrl: 'test-queue',
            VisibilityTimeout: 30,
            WaitTimeSeconds: 5,
        });
    });

    it('should configure visibilityTimeout', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({
                promise: () => Promise.resolve({}),
            })),
        };

        // When
        await sqsReceiveMessages('test-queue', sqs, { visibilityTimeout: 3 });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledWith({
            AttributeNames: ['All'],
            MaxNumberOfMessages: 10,
            MessageAttributeNames: ['All'],
            QueueUrl: 'test-queue',
            VisibilityTimeout: 3,
            WaitTimeSeconds: 2,
        });
    });

    it('should configure waitTime', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({
                promise: () => Promise.resolve({}),
            })),
        };

        // When
        await sqsReceiveMessages('test-queue', sqs, { waitTime: 15 });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledWith({
            AttributeNames: ['All'],
            MaxNumberOfMessages: 10,
            MessageAttributeNames: ['All'],
            QueueUrl: 'test-queue',
            VisibilityTimeout: 30,
            WaitTimeSeconds: 15,
        });
    });

    it('should error if visibilityTimeout too low', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({
                promise: () => Promise.resolve({}),
            })),
        };

        // When
        let error;

        try {
            await sqsReceiveMessages('test-queue', sqs, { visibilityTimeout: 0 });
        } catch (err) {
            error = err;
        }

        // Then
        expect(error).not.toBeUndefined();
        expect(error.message).toBe('Visibility timeout too low');
    });
});

describe('sqsSendMessages', () => {
    it('should send messages to queue', async () => {
        // Given
        const messages = [
            {
                MessageId: 'messageId1',
                Body: 'body1',
                MessageAttributes: undefined,
            },
            {
                MessageId: 'messageId2',
                Body: 'body2',
                MessageAttributes: {
                    attribute1: {
                        DataType: 'String',
                        StringValue: 'string value',
                        BinaryValue: undefined,
                    },
                    attribute2: {
                        DataType: 'Binary',
                        StringValue: undefined,
                        BinaryValue: 'binaryvalue',
                    },
                },
            },
        ];

        const sqs = {
            sendMessageBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [],
                    }),
            })),
        };

        // When
        const successCount = await sqsSendMessages(messages, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(2);

        expect(sqs.sendMessageBatch).toHaveBeenCalledWith({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: 'messageId1',
                    MessageBody: 'body1',
                    MessageAttributes: undefined,
                },
                {
                    Id: 'messageId2',
                    MessageBody: 'body2',
                    MessageAttributes: {
                        attribute1: {
                            DataType: 'String',
                            StringValue: 'string value',
                            BinaryValue: undefined,
                        },
                        attribute2: {
                            DataType: 'Binary',
                            StringValue: undefined,
                            BinaryValue: 'binaryvalue',
                        },
                    },
                },
            ],
        });
    });

    it('should log failed messages', async () => {
        // Given
        const messages = [
            {
                MessageId: 'messageId1',
                Body: 'body1',
            },
            {
                MessageId: 'messageId2',
                Body: 'body2',
            },
        ];

        const sqs = {
            sendMessageBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [
                            {
                                Id: 'messageId2',
                            },
                        ],
                    }),
            })),
        };

        // When
        const successCount = await sqsSendMessages(messages, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(1);
        expect(global.console.error).toHaveBeenCalledWith('Failed to send messageId2');
    });
});

describe('sqsDeleteMessages', () => {
    it('should delete messages from queue', async () => {
        // Given
        const messages = [
            {
                MessageId: 'messageId1',
                ReceiptHandle: 'receipthandle1',
            },
            {
                MessageId: 'messageId2',
                ReceiptHandle: 'receipthandle2',
            },
        ];

        const sqs = {
            deleteMessageBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [],
                    }),
            })),
        };

        // When
        const successCount = await sqsDeleteMessages(messages, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(2);

        expect(sqs.deleteMessageBatch).toHaveBeenCalledWith({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: 'messageId1',
                    ReceiptHandle: 'receipthandle1',
                },
                {
                    Id: 'messageId2',
                    ReceiptHandle: 'receipthandle2',
                },
            ],
        });
    });

    it('should log failed messages', async () => {
        // Given
        const messages = [
            {
                MessageId: 'messageId1',
                Body: 'body1',
            },
            {
                MessageId: 'messageId2',
                Body: 'body2',
            },
        ];

        const sqs = {
            deleteMessageBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [
                            {
                                Id: 'messageId2',
                            },
                        ],
                    }),
            })),
        };

        // When
        const successCount = await sqsDeleteMessages(messages, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(1);
        expect(global.console.error).toHaveBeenCalledWith('Failed to delete messageId2');
    });
});

describe('sqsChangeVisibilityTimeout', () => {
    it('should change message visibity of messages on queue', async () => {
        // Given
        const reciptHandles = ['receipthandle1', 'receipthandle2'];

        const sqs = {
            changeMessageVisibilityBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [],
                    }),
            })),
        };

        // When
        const successCount = await sqsChangeVisibilityTimeout(reciptHandles, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(2);

        expect(sqs.changeMessageVisibilityBatch).toHaveBeenCalledWith({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '0',
                    ReceiptHandle: 'receipthandle1',
                    VisibilityTimeout: 0,
                },
                {
                    Id: '1',
                    ReceiptHandle: 'receipthandle2',
                    VisibilityTimeout: 0,
                },
            ],
        });
    });

    it('should return count of failed updates', async () => {
        // Given
        const reciptHandles = ['receipthandle1', 'receipthandle2'];

        const sqs = {
            changeMessageVisibilityBatch: jest.fn(() => ({
                promise: () =>
                    Promise.resolve({
                        Failed: [
                            {
                                Id: '1',
                            },
                        ],
                    }),
            })),
        };

        // When
        const successCount = await sqsChangeVisibilityTimeout(reciptHandles, 'test-queue', sqs);

        // Then
        expect(successCount).toEqual(1);
    });
});

describe('listQueues', () => {
    it('should list queues', async () => {
        // Given
        const sqs = {
            listQueues: () => ({
                promise: () =>
                    Promise.resolve({
                        QueueUrls: ['queue1', 'queue2'],
                    }),
            }),
        };

        // When
        const queues = await listQueues(sqs);

        // Then
        expect(queues).toEqual(['queue1', 'queue2']);
    });
});

describe('describeQueue', () => {
    it('should describe queue', async () => {
        // Given
        const queueAttributes = {
            ApproximateNumberOfMessages: 12,
            ApproximateNumberOfMessagesDelayed: 0,
            ApproximateNumberOfMessagesNotVisible: 0,
        };

        const sqs = {
            getQueueAttributes: () => ({
                promise: () => Promise.resolve(queueAttributes),
            }),
        };

        // When
        const data = await describeQueue('test-queue', sqs);

        // Then
        expect(data).toEqual(queueAttributes);
    });
});
