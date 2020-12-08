const fs = require('fs');
const { Readable, PassThrough } = require('stream');

const SqsReceive = require('../SqsReceive');
const SqsModify = require('../SqsModify');

const sqs = jest.fn();
const mockListQueues = jest.fn();
const mockDescribeQueue = jest.fn();

jest.mock('fs');
jest.mock('../SqsReceive');
jest.mock('../SqsModify');
jest.mock('../sqsClientUtils', () => ({
    listQueues: async (awsSqs) => mockListQueues(awsSqs),
    describeQueue: async (queue, awsSqs) => mockDescribeQueue(queue, awsSqs),
}));

const sqsUtility = require('../sqsUtility');

beforeEach(() => {
    jest.clearAllMocks();
    global.console.log = jest.fn();
    global.console.error = jest.fn();
    // global.console.log = jest.fn(global.console.log);
});

describe('sqsUtility', () => {
    it('should log error if no action specified', async () => {
        // Given
        const options = {};

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(global.console.log).toHaveBeenCalledWith('Error: Action not specified');
    });
});

describe('sqsUtility queues action', () => {
    it('should list the SQS queues', async () => {
        // Given
        const options = {
            queues: true,
        };

        mockListQueues.mockReturnValueOnce(['queue1', 'queue2']);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(mockListQueues).toHaveBeenCalledWith(sqs);

        expect(global.console.log).toHaveBeenNthCalledWith(1, 'queue1');
        expect(global.console.log).toHaveBeenNthCalledWith(2, 'queue2');
    });
});

describe('sqsUtility describe action', () => {
    it('should describe a SQS queue', async () => {
        // Given
        const options = {
            describe: 'myQueue',
        };

        mockDescribeQueue.mockReturnValueOnce({
            Attributes: {
                ApproximateNumberOfMessages: 3,
                ApproximateNumberOfMessagesDelayed: 4,
                ApproximateNumberOfMessagesNotVisible: 5,
            },
        });

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(mockDescribeQueue).toHaveBeenCalledWith('myQueue', sqs);

        expect(global.console.log).toHaveBeenNthCalledWith(1, 'Queue: myQueue');
        expect(global.console.log).toHaveBeenNthCalledWith(2, 'ApproximateNumberOfMessages: 3');
        expect(global.console.log).toHaveBeenNthCalledWith(3, 'ApproximateNumberOfMessagesDelayed: 4');
        expect(global.console.log).toHaveBeenNthCalledWith(4, 'ApproximateNumberOfMessagesNotVisible: 5');
    });
});

describe('sqsUtility list/extract action', () => {
    it('should error if file exists', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
        };

        fs.existsSync.mockReturnValueOnce(true);

        // When
        let error;

        try {
            await sqsUtility(sqs, options);
        } catch (err) {
            error = err;
        }

        // Then
        expect(error).toBeDefined();
        expect(error.message).toBe('file.csv already exists');
    });

    it('should write messages to CSV', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
        };

        const writeDataFn = jest.fn();
        const writeStream = new PassThrough();
        writeStream.on('data', writeDataFn);
        fs.createWriteStream.mockReturnValueOnce(writeStream);

        const sqsReceiveMock = {
            receiveMessages: jest.fn((queue, writer) => {
                writer([
                    { Row: 'row 1', Value: 'a value' },
                    { Row: 'row 2', Value: 'comma,separated,value' },
                    { Row: 'row 3', Value: '"quoted value"' },
                ]);
            }),
        };
        SqsReceive.mockImplementation(() => sqsReceiveMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsReceiveMock.receiveMessages).toHaveBeenCalledTimes(1);
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][2]).toBe(false);

        expect(writeDataFn.mock.calls[0][0].toString()).toBe('Row,Value,MessageAttributes');
        expect(writeDataFn.mock.calls[1][0].toString()).toBe('\nrow 1,a value,');
        expect(writeDataFn.mock.calls[2][0].toString()).toBe('\nrow 2,"comma,separated,value",');
        expect(writeDataFn.mock.calls[3][0].toString()).toBe('\nrow 3,"""quoted value""",');
    });

    it('should JSON stringify message attributes in CSV', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
        };

        const writeDataFn = jest.fn();
        const writeStream = new PassThrough();
        writeStream.on('data', writeDataFn);
        fs.createWriteStream.mockReturnValueOnce(writeStream);

        const MessageAttributes = { type: 'String', stringValue: 'a string' };
        const sqsReceiveMock = {
            receiveMessages: jest.fn((queue, writer) => {
                writer([{ Row: 'row 1', MessageAttributes }, { Row: 'row 2' }]);
            }),
        };
        SqsReceive.mockImplementation(() => sqsReceiveMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(writeDataFn.mock.calls[0][0].toString()).toBe('Row,MessageAttributes');
        expect(writeDataFn.mock.calls[1][0].toString()).toBe(
            `\nrow 1,"${JSON.stringify(MessageAttributes).replace(/"/g, '""')}"`
        );
        expect(writeDataFn.mock.calls[2][0].toString()).toBe('\nrow 2,');
    });

    it('should create a filter message processor', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
            filter: 'message.Value === "filter"',
        };

        fs.createWriteStream.mockReturnValueOnce(new PassThrough());

        let messageProcessor;
        const sqsReceiveMock = {
            receiveMessages: jest.fn(),
        };
        SqsReceive.mockImplementation((_sqs, opts) => {
            messageProcessor = opts.messageProcessor;
            return sqsReceiveMock;
        });

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(messageProcessor({ Row: '1', Value: 'filter' })).toEqual({ Row: '1', Value: 'filter' });
        expect(messageProcessor({ Row: '2', Value: 'ignore' })).toBeNull();
    });

    it('should create a transform message processor', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
            transform: 'message.Value = "transformed " + message.Row',
        };

        fs.createWriteStream.mockReturnValueOnce(new PassThrough());

        let messageProcessor;
        const sqsReceiveMock = {
            receiveMessages: jest.fn(),
        };
        SqsReceive.mockImplementation((_sqs, opts) => {
            messageProcessor = opts.messageProcessor;
            return sqsReceiveMock;
        });

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(messageProcessor({ Row: '1', Value: 'original' })).toEqual({ Row: '1', Value: 'transformed 1' });
    });

    it('should log receive counts (list)', async () => {
        // Given
        const options = {
            list: 'myQueue',
            file: 'file.csv',
        };

        fs.createWriteStream.mockReturnValueOnce(new PassThrough());

        const sqsReceiveMock = {
            receiveMessages: jest.fn(),
            receiveCount: 5,
            writeCount: 4,
        };
        SqsReceive.mockImplementation(() => sqsReceiveMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsReceiveMock.receiveMessages).toHaveBeenCalledTimes(1);
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][2]).toBe(false);

        expect(global.console.log).toHaveBeenNthCalledWith(1, '5 messages received from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '4 messages written to file');
    });

    it('should log receive counts (extract)', async () => {
        // Given
        const options = {
            extract: 'myQueue',
            file: 'file.csv',
        };

        fs.createWriteStream.mockReturnValueOnce(new PassThrough());

        const sqsReceiveMock = {
            receiveMessages: jest.fn(),
            receiveCount: 8,
            deleteCount: 2,
            writeCount: 5,
        };
        SqsReceive.mockImplementation(() => sqsReceiveMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsReceiveMock.receiveMessages).toHaveBeenCalledTimes(1);
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsReceiveMock.receiveMessages.mock.calls[0][2]).toBe(true);

        expect(global.console.log).toHaveBeenNthCalledWith(1, '8 messages received from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '5 messages written to file');
        expect(global.console.log).toHaveBeenNthCalledWith(3, '2 messages deleted from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(4, '6 messages failed to delete from queue');
    });

    it('should log receive counts (error)', async () => {
        // Given
        const options = {
            extract: 'myQueue',
            file: 'file.csv',
        };

        fs.createWriteStream.mockReturnValueOnce(new PassThrough());

        const sqsReceiveMock = {
            receiveMessages: jest.fn(() => {
                throw new Error('Some error');
            }),
            receiveCount: 8,
            deleteCount: 2,
            writeCount: 5,
        };
        SqsReceive.mockImplementation(() => sqsReceiveMock);

        // When
        let error;

        try {
            await sqsUtility(sqs, options);
        } catch (err) {
            error = err;
        }

        // Then
        expect(error).toBeDefined();
        expect(error.message).toBe('Some error');

        expect(global.console.log).toHaveBeenNthCalledWith(1, '8 messages received from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '5 messages written to file');
        expect(global.console.log).toHaveBeenNthCalledWith(3, '2 messages deleted from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(4, '6 messages failed to delete from queue');
    });
});

describe('sqsUtility load/delete action', () => {
    it('should write messages to CSV', async () => {
        // Given
        const options = {
            load: 'myQueue',
            file: 'file.csv',
        };

        fs.createReadStream.mockReturnValueOnce(
            Readable.from('Row,Value\nrow 1,value 1\nrow 2,"comma,separated,value"\nrow 3,"""quoted value value"""')
        );

        const modifyDataFn = jest.fn();
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', modifyDataFn).on('end', () => resolve());
                });
            }),
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsModifyMock.modifyMessages).toHaveBeenCalledTimes(1);
        expect(sqsModifyMock.modifyMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsModifyMock.modifyMessages.mock.calls[0][2]).toBe(false);

        expect(modifyDataFn.mock.calls[0][0]).toEqual({ Row: 'row 1', Value: 'value 1' });
        expect(modifyDataFn.mock.calls[1][0]).toEqual({ Row: 'row 2', Value: 'comma,separated,value' });
        expect(modifyDataFn.mock.calls[2][0]).toEqual({ Row: 'row 3', Value: '"quoted value value"' });
    });

    it('should JSON parse message attributes in CSV', async () => {
        // Given
        const options = {
            load: 'myQueue',
            file: 'file.csv',
        };

        const MessageAttributes = { type: 'String', stringValue: 'a string' };
        fs.createReadStream.mockReturnValueOnce(
            Readable.from(
                'Row,MessageAttributes\n' +
                    `row 1,"${JSON.stringify(MessageAttributes).replace(/"/g, '""')}"\n` +
                    'row 2,'
            )
        );

        const modifyDataFn = jest.fn();
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', modifyDataFn).on('end', () => resolve());
                });
            }),
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsModifyMock.modifyMessages).toHaveBeenCalledTimes(1);
        expect(sqsModifyMock.modifyMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsModifyMock.modifyMessages.mock.calls[0][2]).toBe(false);

        expect(modifyDataFn.mock.calls[0][0]).toEqual({ Row: 'row 1', MessageAttributes });
        expect(modifyDataFn.mock.calls[1][0]).toEqual({ Row: 'row 2' });
    });

    it('should nullify invalid JSON message attributes in CSV', async () => {
        // Given
        const options = {
            load: 'myQueue',
            file: 'file.csv',
        };

        fs.createReadStream.mockReturnValueOnce(
            Readable.from('Row,MessageAttributes\nrow 1,\nrow 2,"Invalid: Json"\nrow 3,')
        );

        const modifyDataFn = jest.fn();
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', modifyDataFn).on('end', () => resolve());
                });
            }),
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(modifyDataFn.mock.calls[0][0]).toEqual({ Row: 'row 1' });
        expect(modifyDataFn.mock.calls[1][0]).toEqual({ Row: 'row 2', MessageAttributes: null });
        expect(modifyDataFn.mock.calls[2][0]).toEqual({ Row: 'row 3' });
        expect(modifyDataFn.mock.calls.length).toBe(3);
    });

    it('should create a filter+transform message processor', async () => {
        // Given
        const options = {
            load: 'myQueue',
            file: 'file.csv',
            filter: 'message.Value === "filter"',
            transform: 'message.Value = "transformed " + message.Row',
        };

        fs.createReadStream.mockReturnValueOnce(Readable.from('Row,Value\nrow 1,value'));

        let messageProcessor;
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', () => {}).on('end', () => resolve());
                });
            }),
        };
        SqsModify.mockImplementation((_sqs, opts) => {
            messageProcessor = opts.messageProcessor;
            return sqsModifyMock;
        });

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(messageProcessor({ Row: '1', Value: 'filter' })).toEqual({ Row: '1', Value: 'transformed 1' });
        expect(messageProcessor({ Row: '2', Value: 'ignore' })).toBeNull();
    });

    it('should log modified counts (load)', async () => {
        // Given
        const options = {
            load: 'myQueue',
            file: 'file.csv',
        };

        fs.createReadStream.mockReturnValueOnce(Readable.from('Row,Value\nrow 1,"value 1"'));
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', () => {}).on('end', resolve);
                });
            }),
            readCount: 5,
            modifiedCount: 4,
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsModifyMock.modifyMessages).toHaveBeenCalledTimes(1);
        expect(sqsModifyMock.modifyMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsModifyMock.modifyMessages.mock.calls[0][2]).toBe(false);

        expect(global.console.log).toHaveBeenNthCalledWith(1, '5 messages read from file');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '4 messages sent to queue');
        expect(global.console.log).toHaveBeenNthCalledWith(3, '1 messages failed to send to queue');
    });

    it('should log modified counts (delete)', async () => {
        // Given
        const options = {
            delete: 'myQueue',
            file: 'file.csv',
        };

        fs.createReadStream.mockReturnValueOnce(Readable.from('Row,Value\nrow 1,"value 1"'));
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve) => {
                    reader.on('data', () => {}).on('end', resolve);
                });
            }),
            readCount: 7,
            modifiedCount: 2,
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        await sqsUtility(sqs, options);

        // Then
        expect(sqsModifyMock.modifyMessages).toHaveBeenCalledTimes(1);
        expect(sqsModifyMock.modifyMessages.mock.calls[0][0]).toBe('myQueue');
        expect(sqsModifyMock.modifyMessages.mock.calls[0][2]).toBe(true);

        expect(global.console.log).toHaveBeenNthCalledWith(1, '7 messages read from file');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '2 messages deleted from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(3, '5 messages failed to delete from queue');
    });

    it('should log modified counts (error)', async () => {
        // Given
        const options = {
            delete: 'myQueue',
            file: 'file.csv',
        };

        fs.createReadStream.mockReturnValueOnce(Readable.from('Row,Value\nrow 1,"value 1"'));
        const sqsModifyMock = {
            modifyMessages: jest.fn((queue, reader) => {
                return new Promise((resolve, reject) => {
                    reader.on('data', () => {}).on('end', () => reject(new Error('Some error')));
                });
            }),
            readCount: 7,
            modifiedCount: 2,
        };
        SqsModify.mockImplementation(() => sqsModifyMock);

        // When
        let error;

        try {
            await sqsUtility(sqs, options);
        } catch (err) {
            error = err;
        }

        // Then
        expect(error).toBeDefined();
        expect(error.message).toBe('Some error');

        expect(global.console.log).toHaveBeenNthCalledWith(1, '7 messages read from file');
        expect(global.console.log).toHaveBeenNthCalledWith(2, '2 messages deleted from queue');
        expect(global.console.log).toHaveBeenNthCalledWith(3, '5 messages failed to delete from queue');
    });
});
