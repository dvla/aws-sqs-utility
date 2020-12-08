const { Readable } = require('stream');

const sqs = jest.fn();
const mockSendMessages = jest.fn();
const mockDeleteMessages = jest.fn();

jest.mock('../sqsClientUtils', () => ({
    sqsSendMessages: async (messages, queue, awsSqs) => mockSendMessages(messages, queue, awsSqs),
    sqsDeleteMessages: async (messages, queue, awsSqs) => mockDeleteMessages(messages, queue, awsSqs),
}));

const SqsModify = require('../SqsModify');

beforeEach(() => {
    jest.clearAllMocks();
    global.console.error = jest.fn();
    // global.console.error = jest.fn(global.console.error);
});

describe('modifyMessages', () => {
    it('should send messages to queue (half batch)', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 5 messages (1 batch)
        const messages = [...Array(5).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(5));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, messages, 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(5);
        expect(sqsModify.modifiedCount).toEqual(5);
    });

    it('should send messages to queue (full batch)', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 10 messages (1 batch)
        const messages = [...Array(10).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(10));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, messages, 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(10);
        expect(sqsModify.modifiedCount).toEqual(10);
    });

    it('should send messages to queue (multiple batches)', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 15 messages (2 batches, 10+5)
        const messages = [...Array(15).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(10)).mockReturnValueOnce(Promise.resolve(5));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, messages.slice(0, 10), 'myQueue', sqs);
        expect(mockSendMessages).toHaveBeenNthCalledWith(2, messages.slice(10, 15), 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(15);
        expect(sqsModify.modifiedCount).toEqual(15);
    });

    it('should ignore empty messages', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 10 messages (1 batch)
        const messages = [
            {
                MessageId: 'message1',
            },
            {},
            {
                MessageId: 'message2',
            },
        ];

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(2));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, [messages[0], messages[2]], 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(3);
        expect(sqsModify.modifiedCount).toEqual(2);

        expect(global.console.error).toHaveBeenCalledWith('Ignoring empty message (row 2)');
    });

    it('should ignore invalid messages', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 10 messages (1 batch)
        const messages = [
            {
                MessageId: 'message1',
            },
            {
                MessageId: 'invalid message',
            },
            {
                MessageId: 'message2',
            },
        ];

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(2));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, [messages[0], messages[2]], 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(3);
        expect(sqsModify.modifiedCount).toEqual(2);

        expect(global.console.error).toHaveBeenCalledWith('Ignoring invalid message (row 2)');
    });

    it('should ignore messages with null message attributes', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 10 messages (1 batch)
        const messages = [
            {
                MessageId: 'message1',
            },
            {
                MessageId: 'message2',
                MessageAttributes: null,
            },
            {
                MessageId: 'message3',
            },
        ];

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(2));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, [messages[0], messages[2]], 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(3);
        expect(sqsModify.modifiedCount).toEqual(2);

        expect(global.console.error).toHaveBeenCalledWith('Ignoring message due to invalid message attributes (row 2)');
    });

    it('should delete messages to queue (single batch)', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        const message1 = {
            MessageId: 'message1',
        };
        const message2 = {
            MessageId: 'message2',
        };

        const reader = Readable.from([message1, message2]);

        mockDeleteMessages.mockReturnValueOnce(Promise.resolve(2));

        // When
        await sqsModify.modifyMessages('myQueue', reader, true);

        // Then
        expect(mockDeleteMessages).toHaveBeenNthCalledWith(1, [message1, message2], 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(2);
        expect(sqsModify.modifiedCount).toEqual(2);
    });

    it('should delete messages from queue (multiple batches)', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 15 messages (2 batches, 10+5)
        const messages = [...Array(15).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockDeleteMessages.mockReturnValueOnce(Promise.resolve(10)).mockReturnValueOnce(Promise.resolve(5));

        // When
        await sqsModify.modifyMessages('myQueue', reader, true);

        // Then
        expect(mockDeleteMessages).toHaveBeenNthCalledWith(1, messages.slice(0, 10), 'myQueue', sqs);
        expect(mockDeleteMessages).toHaveBeenNthCalledWith(2, messages.slice(10, 15), 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(15);
        expect(sqsModify.modifiedCount).toEqual(15);
    });

    it('should filter messages', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {
            messageProcessor: (message) => (message.MessageId === 'message2' ? message : null),
        });

        const messages = [
            {
                MessageId: 'message1',
            },
            {
                MessageId: 'message2',
            },
            {
                MessageId: 'message3',
            },
        ];

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(1));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(1, [messages[1]], 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(3);
        expect(sqsModify.filteredCount).toEqual(1);
        expect(sqsModify.modifiedCount).toEqual(1);
    });

    it('should transform messages', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {
            messageProcessor: (message) => {
                return {
                    ...message,
                    MessageId: `transformed-${message.MessageId}`,
                };
            },
        });

        const messages = [
            {
                MessageId: 'message1',
            },
            {
                MessageId: 'message2',
            },
        ];

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(2));

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(mockSendMessages).toHaveBeenNthCalledWith(
            1,
            [
                {
                    MessageId: 'transformed-message1',
                },
                {
                    MessageId: 'transformed-message2',
                },
            ],
            'myQueue',
            sqs
        );

        expect(sqsModify.readCount).toEqual(2);
        expect(sqsModify.filteredCount).toEqual(2);
        expect(sqsModify.modifiedCount).toEqual(2);
    });

    it('should handle error on data', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 25 messages (3 batches, 10+10+5)
        const messages = [...Array(25).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockSendMessages.mockReturnValueOnce(Promise.resolve(10)).mockImplementationOnce(() => {
            throw new Error('dummy error');
        });

        // When
        let error;

        try {
            await sqsModify.modifyMessages('myQueue', reader, false);
        } catch (err) {
            error = err;
        }

        // Then
        expect(error).toBe('Aborted: Error: dummy error');
        expect(global.console.error).toHaveBeenCalledWith('Error (row batch 11-20): dummy error');

        expect(mockSendMessages).toHaveBeenNthCalledWith(1, messages.slice(0, 10), 'myQueue', sqs);
        expect(mockSendMessages).toHaveBeenNthCalledWith(2, messages.slice(10, 20), 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(20);
        expect(sqsModify.modifiedCount).toEqual(10);
    });

    it('should handle error on end', async () => {
        // Given
        const sqsModify = new SqsModify(sqs, {});

        // 5 messages (1 batch)
        const messages = [...Array(5).keys()].map((key) => ({
            MessageId: `message${key + 1}`,
        }));

        const reader = Readable.from(messages);

        mockSendMessages.mockImplementationOnce(() => {
            throw new Error('dummy error');
        });

        // When
        await sqsModify.modifyMessages('myQueue', reader, false);

        // Then
        expect(global.console.error).toHaveBeenCalledWith('Error (row batch 1-5): dummy error');

        expect(mockSendMessages).toHaveBeenNthCalledWith(1, messages.slice(0, 5), 'myQueue', sqs);

        expect(sqsModify.readCount).toEqual(5);
        expect(sqsModify.modifiedCount).toEqual(0);
    });
});
