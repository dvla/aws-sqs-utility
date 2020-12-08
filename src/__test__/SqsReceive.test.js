const sqs = jest.fn();
const mockReceiveMessages = jest.fn();
const mockDeleteMessages = jest.fn();
const mockChangeVisibilityTimeout = jest.fn();

jest.mock('../sqsClientUtils', () => ({
    MIN_VISIBILITY_TIMEOUT: 1,
    sqsReceiveMessages: async (queue, awsSqs, options) => mockReceiveMessages(queue, awsSqs, options),
    sqsDeleteMessages: async (messages, queue, awsSqs) => mockDeleteMessages(messages, queue, awsSqs),
    sqsChangeVisibilityTimeout: async (handles, queue, awsSqs) => mockChangeVisibilityTimeout(handles, queue, awsSqs),
}));

const SqsReceive = require('../SqsReceive');

const blockingSleep = (millis) => {
    const start = Date.now();
    // eslint-disable-next-line no-empty
    do {} while (Date.now() - start < millis);
};

beforeEach(() => {
    jest.clearAllMocks();
    global.console.warn = jest.fn();
});

describe('receiveMessages', () => {
    it('should write received messages (single batch)', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {});

        const message1 = {
            Body: 'message 1',
            ReceiptHandle: 'handle 1',
        };
        const message2 = {
            Body: 'message 2',
            ReceiptHandle: 'handle 2',
        };

        mockReceiveMessages.mockReturnValueOnce([message1, message2]).mockReturnValueOnce([]);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [message1, message2]);

        const handles = [message1.ReceiptHandle, message2.ReceiptHandle];
        expect(mockChangeVisibilityTimeout).toHaveBeenNthCalledWith(1, handles, 'myQueue', sqs);

        expect(mockDeleteMessages).not.toBeCalled();

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(0);
    });

    it('should write received messages (multiple batches)', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {});

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };
        const message3 = { Body: 'message 3' };

        mockReceiveMessages
            .mockReturnValueOnce([message1])
            .mockReturnValueOnce([message2, message3])
            .mockReturnValueOnce([]);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [message1]);
        expect(writer).toHaveBeenNthCalledWith(2, [message2, message3]);

        const handles1 = [message1.ReceiptHandle];
        const handles2 = [message2.ReceiptHandle, message3.ReceiptHandle];
        expect(mockChangeVisibilityTimeout).toHaveBeenNthCalledWith(1, handles1, 'myQueue', sqs);
        expect(mockChangeVisibilityTimeout).toHaveBeenNthCalledWith(2, handles2, 'myQueue', sqs);

        expect(mockDeleteMessages).not.toBeCalled();

        expect(sqsReceive.receiveCount).toEqual(3);
        expect(sqsReceive.deleteCount).toEqual(0);
    });

    it('should delete received messages from queue (single batch)', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {});

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };

        mockReceiveMessages.mockReturnValueOnce([message1, message2]).mockReturnValueOnce([]);
        mockDeleteMessages.mockReturnValueOnce(2);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, true);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [message1, message2]);

        expect(mockDeleteMessages).toHaveBeenNthCalledWith(1, [message1, message2], 'myQueue', sqs);

        expect(mockChangeVisibilityTimeout).not.toBeCalled();

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(2);
    });

    it('should delete received messages from queue (multiple batches)', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {});

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };
        const message3 = { Body: 'message 3' };

        mockReceiveMessages
            .mockReturnValueOnce([message1])
            .mockReturnValueOnce([message2, message3])
            .mockReturnValueOnce([]);
        mockDeleteMessages.mockReturnValueOnce(1).mockReturnValueOnce(2);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, true);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [message1]);
        expect(writer).toHaveBeenNthCalledWith(2, [message2, message3]);

        expect(mockDeleteMessages).toHaveBeenNthCalledWith(1, [message1], 'myQueue', sqs);
        expect(mockDeleteMessages).toHaveBeenNthCalledWith(2, [message2, message3], 'myQueue', sqs);

        expect(mockChangeVisibilityTimeout).not.toBeCalled();

        expect(sqsReceive.receiveCount).toEqual(3);
        expect(sqsReceive.deleteCount).toEqual(3);
    });

    it('should apply message limit', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, { limit: 2 });

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };

        mockReceiveMessages.mockReturnValueOnce([message1]).mockReturnValueOnce([message2]);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(1, 'myQueue', sqs, {
            maxNumberOfMessages: 2,
            visibilityTimeout: 29,
        });
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(2, 'myQueue', sqs, {
            maxNumberOfMessages: 1,
            visibilityTimeout: 29,
        });

        expect(writer).toHaveBeenNthCalledWith(1, [message1]);
        expect(writer).toHaveBeenNthCalledWith(2, [message2]);
        expect(writer).toHaveBeenCalledTimes(2);

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(0);
    });

    it('should apply timeout', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, { timeout: 5 });

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };

        mockReceiveMessages
            .mockImplementationOnce(() => {
                blockingSleep(1000);
                return [message1];
            })
            .mockImplementationOnce(() => {
                blockingSleep(2000);
                return [message2];
            })
            .mockReturnValueOnce([]);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(1, 'myQueue', sqs, {
            maxNumberOfMessages: 1000,
            visibilityTimeout: 4,
        });
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(2, 'myQueue', sqs, {
            maxNumberOfMessages: 999,
            visibilityTimeout: 3,
        });
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(3, 'myQueue', sqs, {
            maxNumberOfMessages: 998,
            visibilityTimeout: 1,
        });

        expect(writer).toHaveBeenNthCalledWith(1, [message1]);
        expect(writer).toHaveBeenNthCalledWith(2, [message2]);
        expect(writer).toHaveBeenCalledTimes(2);

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(0);
    });

    it('should exit with warning if timeout reached', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, { timeout: 2 });

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };

        mockReceiveMessages.mockReturnValueOnce([message1]).mockImplementationOnce(() => {
            blockingSleep(1000);
            return [message2];
        });

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(1, 'myQueue', sqs, {
            maxNumberOfMessages: 1000,
            visibilityTimeout: 1,
        });
        expect(mockReceiveMessages).toHaveBeenNthCalledWith(2, 'myQueue', sqs, {
            maxNumberOfMessages: 999,
            visibilityTimeout: 1,
        });
        expect(mockReceiveMessages).toHaveBeenCalledTimes(2);

        expect(writer).toHaveBeenNthCalledWith(1, [message1]);
        expect(writer).toHaveBeenNthCalledWith(2, [message2]);
        expect(writer).toHaveBeenCalledTimes(2);

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(0);

        expect(global.console.warn).toBeCalledTimes(1);
        expect(global.console.warn).toHaveBeenCalledWith('Warning: Timeout reached (2 seconds)');
    });

    it('should filter messages', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {
            messageProcessor: (message) =>
                message.Body === 'message 2' || message.Body === 'message 4' ? message : null,
        });

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };
        const message3 = { Body: 'message 3' };
        const message4 = { Body: 'message 4' };

        mockReceiveMessages
            .mockReturnValueOnce([message1])
            .mockReturnValueOnce([message2, message3])
            .mockReturnValueOnce([message4])
            .mockReturnValueOnce([]);
        mockDeleteMessages.mockReturnValueOnce(1).mockReturnValueOnce(1);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, true);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [message2]);
        expect(writer).toHaveBeenNthCalledWith(2, [message4]);

        const handles1 = [message1.ReceiptHandle];
        const handles2 = [message2.ReceiptHandle, message3.ReceiptHandle];
        expect(mockChangeVisibilityTimeout).toHaveBeenNthCalledWith(1, handles1, 'myQueue', sqs);
        expect(mockChangeVisibilityTimeout).toHaveBeenNthCalledWith(2, handles2, 'myQueue', sqs);
        expect(mockChangeVisibilityTimeout).toHaveBeenCalledTimes(2);

        expect(sqsReceive.receiveCount).toEqual(4);
        expect(sqsReceive.filteredCount).toEqual(2);
        expect(sqsReceive.writeCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(2);
    });

    it('should transform messages', async () => {
        // Given
        const writer = jest.fn();
        const sqsReceive = new SqsReceive(sqs, {
            messageProcessor: (message) => {
                return {
                    ...message,
                    Body: `transformed ${message.Body}`,
                };
            },
        });

        const message1 = { Body: 'message 1' };
        const message2 = { Body: 'message 2' };

        mockReceiveMessages.mockReturnValueOnce([message1]).mockReturnValueOnce([message2]).mockReturnValueOnce([]);

        // When
        await sqsReceive.receiveMessages('myQueue', writer, false);

        // Then
        expect(writer).toHaveBeenNthCalledWith(1, [
            {
                Body: 'transformed message 1',
            },
        ]);
        expect(writer).toHaveBeenNthCalledWith(2, [
            {
                Body: 'transformed message 2',
            },
        ]);

        expect(sqsReceive.receiveCount).toEqual(2);
        expect(sqsReceive.filteredCount).toEqual(2);
        expect(sqsReceive.writeCount).toEqual(2);
        expect(sqsReceive.deleteCount).toEqual(0);
    });
});
