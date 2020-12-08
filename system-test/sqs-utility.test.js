const AWS = require('aws-sdk');
const uuid = require('uuid/v4');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const sqsEndpoint = 'http://localhost:4576';
const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    endpoint: sqsEndpoint,
    region: 'eu-west-2',
});

jest.setTimeout(30000);
const tempdir = fs.mkdtempSync('test-');

let queueUrl1;
let queueUrl2;

beforeEach(async () => {
    const response1 = await sqs.createQueue({ QueueName: uuid() }).promise();
    const response2 = await sqs.createQueue({ QueueName: uuid() }).promise();
    queueUrl1 = response1.QueueUrl;
    queueUrl2 = response2.QueueUrl;
});

afterEach(async () => {
    await sqs.deleteQueue({ QueueUrl: queueUrl1 }).promise();
    await sqs.deleteQueue({ QueueUrl: queueUrl2 }).promise();
});

afterAll(() => {
    fs.rmdirSync(tempdir, { recursive: true });
});

function generateFileName() {
    const filename = uuid();
    return `${tempdir}${path.sep}${filename}`;
}

async function execSqsUtility(args) {
    return new Promise((resolve) => {
        exec(`node . --endpoint ${sqsEndpoint} ${args.join(' ')}`, (error, stdout, stderr) => {
            resolve({ error, stdout, stderr });
        });
    });
}

async function sendMessage(queueUrl, message) {
    return sqs
        .sendMessage({
            QueueUrl: queueUrl,
            MessageBody: message,
        })
        .promise();
}

async function sendMessages(count, queueUrl, message) {
    return Promise.all(
        Array(count)
            .fill()
            .map((_, i) => sendMessage(queueUrl, `${message} ${i}`))
    );
}

async function getMessages(count, queueUrl) {
    const messageArrays = await Promise.all(
        Array(Math.ceil(count / 10))
            .fill()
            .map(() =>
                sqs
                    .receiveMessage({
                        AttributeNames: ['All'],
                        MaxNumberOfMessages: 10,
                        MessageAttributeNames: ['All'],
                        QueueUrl: queueUrl,
                        VisibilityTimeout: 30,
                        WaitTimeSeconds: 5,
                    })
                    .promise()
            )
    );

    return messageArrays.reduce((acc, value) => acc.concat(value.Messages), []);
}

async function getMessageCount(queueUrl) {
    const result = await sqs
        .getQueueAttributes({
            QueueUrl: queueUrl,
            AttributeNames: ['ApproximateNumberOfMessages'],
        })
        .promise();

    return Number(result.Attributes.ApproximateNumberOfMessages);
}

async function getNotVisibleMessageCount(queueUrl) {
    const result = await sqs
        .getQueueAttributes({
            QueueUrl: queueUrl,
            AttributeNames: ['ApproximateNumberOfMessagesNotVisible'],
        })
        .promise();

    return Number(result.Attributes.ApproximateNumberOfMessagesNotVisible);
}

async function assertCsvMessages(filename, receipts, message) {
    const csv = fs.readFileSync(filename, { encoding: 'utf8' });

    receipts.forEach((receipt, i) => {
        expect(csv).toMatch(new RegExp(`${receipt.MessageId}.*${message} ${i}`));
    });
}

async function assertCsvMessageCount(filename, count) {
    const csv = fs.readFileSync(filename, { encoding: 'utf8' });
    const rows = csv.split(/\n/);

    expect(rows.length).toBe(count + 1); // +1 for header row
}

async function assertEmptyFile(filename) {
    const csv = fs.readFileSync(filename, { encoding: 'utf8' });
    expect(csv).toBe('');
}

async function assertQueueMessages(queueMessages, message) {
    const messageLines = queueMessages.map((queueMessage) => queueMessage.Body).join('\n');

    queueMessages.forEach((_, i) => {
        expect(messageLines).toMatch(new RegExp(`${message} ${i}`));
    });
}

describe('sqs-utility', () => {
    it('should fail if no command specified', async () => {
        // Given

        // When
        const result = await execSqsUtility([]);

        // Then
        expect(result.error.code).toBe(1);
    });
});

describe('sqs-utility --queues', () => {
    it('should list queues', async () => {
        // Given

        // When
        const result = await execSqsUtility(['--queues']);

        // Then
        expect(result.stdout).toContain(queueUrl1);
        expect(result.stdout).toContain(queueUrl2);
    });
});

describe('sqs-utility --describe', () => {
    it('should output number of messages on queue', async () => {
        // Given
        await sendMessages(5, queueUrl1, 'test');

        // When
        const result = await execSqsUtility(['--describe', queueUrl1]);

        // Then
        expect(result.stdout).toContain('ApproximateNumberOfMessages: 5');
    });
});

describe('sqs-utility --list', () => {
    it('should list no message on queue', async () => {
        // Given
        const filename = generateFileName();

        // When
        const result = await execSqsUtility(['--list', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('0 messages received from queue');
        expect(result.stdout).toContain('0 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertEmptyFile(filename);
    });

    it('should list single message on queue', async () => {
        // Given
        const filename = generateFileName();
        const receipts = await sendMessages(1, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--list', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('1 messages received from queue');
        expect(result.stdout).toContain('1 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(1);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessages(filename, receipts, 'message');
    });

    it('should list multiple messages on queue', async () => {
        // Given
        const filename = generateFileName();
        const receipts = await sendMessages(15, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--list', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('15 messages received from queue');
        expect(result.stdout).toContain('15 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(15);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessages(filename, receipts, 'message');
    });

    it('should apply message limit', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(15, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--list', queueUrl1, '--file', filename, '--limit', 11]);

        // Then
        expect(result.stdout).toContain('11 messages received from queue');
        expect(result.stdout).toContain('11 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(15);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessageCount(filename, 11);
    });
});

describe('sqs-utility --extract', () => {
    it('should extract no message from queue', async () => {
        // Given
        const filename = generateFileName();

        // When
        const result = await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('0 messages received from queue');
        expect(result.stdout).toContain('0 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertEmptyFile(filename);
    });

    it('should extract single message from queue', async () => {
        // Given
        const filename = generateFileName();
        const receipts = await sendMessages(1, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('1 messages received from queue');
        expect(result.stdout).toContain('1 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessages(filename, receipts, 'message');
    });

    it('should extract multiple messages from queue', async () => {
        // Given
        const filename = generateFileName();
        const receipts = await sendMessages(20, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('20 messages received from queue');
        expect(result.stdout).toContain('20 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessages(filename, receipts, 'message');
    });

    it('should apply message limit', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(15, queueUrl1, 'message');

        // When
        const result = await execSqsUtility(['--extract', queueUrl1, '--file', filename, '--limit', 9]);

        // Then
        expect(result.stdout).toContain('9 messages received from queue');
        expect(result.stdout).toContain('9 messages written to file');
        expect(await getMessageCount(queueUrl1)).toBe(6);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);

        assertCsvMessageCount(filename, 9);
    });
});

describe('sqs-utility --load', () => {
    it('should load no messages onto queue', async () => {
        // Given
        const filename = generateFileName();
        await execSqsUtility(['--extract', queueUrl2, '--file', filename]);

        // When
        const result = await execSqsUtility(['--load', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('0 messages sent to queue');
        expect(result.stdout).toContain('0 messages failed to send to queue');
        expect(await getMessageCount(queueUrl1)).toBe(0);
    });

    it('should load single message onto queue', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(1, queueUrl2, 'message');
        await execSqsUtility(['--extract', queueUrl2, '--file', filename]);

        // When
        const result = await execSqsUtility(['--load', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('1 messages sent to queue');
        expect(result.stdout).toContain('0 messages failed to send to queue');
        expect(await getMessageCount(queueUrl1)).toBe(1);

        const messages = await getMessages(1, queueUrl1);
        assertQueueMessages(messages, 'message');
    });

    it('should load multiple messages onto queue', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(15, queueUrl2, 'message');
        await execSqsUtility(['--extract', queueUrl2, '--file', filename]);

        // When
        const result = await execSqsUtility(['--load', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('15 messages sent to queue');
        expect(result.stdout).toContain('0 messages failed to send to queue');
        expect(await getMessageCount(queueUrl1)).toBe(15);

        const messages = await getMessages(15, queueUrl1);
        assertQueueMessages(messages, 'message');
    });
});

describe('sqs-utility --delete', () => {
    it('should delete no messages from queue', async () => {
        // Given
        const filename = generateFileName();
        await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // When
        const result = await execSqsUtility(['--delete', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('0 messages deleted from queue');
        expect(result.stdout).toContain('0 messages failed to delete from queue');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);
    });

    it('should delete single message from queue', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(1, queueUrl1, 'message');
        await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // When
        const result = await execSqsUtility(['--delete', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('1 messages deleted from queue');
        expect(result.stdout).toContain('0 messages failed to delete from queue');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);
    });

    it('should delete multiple messages from queue', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(20, queueUrl1, 'message');
        await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        // When
        const result = await execSqsUtility(['--delete', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('20 messages deleted from queue');
        expect(result.stdout).toContain('0 messages failed to delete from queue');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);
    });

    it('should delete already deleted messages', async () => {
        // Given
        const filename = generateFileName();
        await sendMessages(10, queueUrl1, 'message');
        await execSqsUtility(['--extract', queueUrl1, '--file', filename]);

        await execSqsUtility(['--delete', queueUrl1, '--file', filename]);

        // When
        const result = await execSqsUtility(['--delete', queueUrl1, '--file', filename]);

        // Then
        expect(result.stdout).toContain('10 messages deleted from queue');
        expect(result.stdout).toContain('0 messages failed to delete from queue');
        expect(await getMessageCount(queueUrl1)).toBe(0);
        expect(await getNotVisibleMessageCount(queueUrl1)).toBe(0);
    });
});
