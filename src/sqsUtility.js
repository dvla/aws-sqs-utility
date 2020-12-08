/* eslint-disable no-console */
const csv = require('fast-csv');
const vm = require('vm');
const fs = require('fs');
const { Transform } = require('stream');

const { listQueues, describeQueue } = require('./sqsClientUtils');
const SqsModify = require('./SqsModify');
const SqsReceive = require('./SqsReceive');

const parseMessageAttributesTransform = () => {
    let rowCount = 0;

    return new Transform({
        objectMode: true,
        transform: (data, encoding, done) => {
            rowCount += 1;

            const message = { ...data };

            if (message.MessageAttributes) {
                try {
                    message.MessageAttributes = JSON.parse(message.MessageAttributes);
                } catch (error) {
                    // eslint-disable-next-line no-console
                    console.error(`Invalid JSON MessageAttributes (row ${rowCount}): ${error.message}`);
                    message.MessageAttributes = null;
                }
            } else {
                delete message.MessageAttributes; // ensure empty string removed
            }

            done(null, message);
        },
    });
};

function stringifyMessageAttributes(message) {
    return {
        ...message,
        MessageAttributes: message.MessageAttributes && JSON.stringify(message.MessageAttributes),
    };
}

function createCsvWriteStream(file) {
    if (fs.existsSync(file)) {
        throw new Error(`${file} already exists`);
    }

    const csvStream = csv.format({ headers: true });
    csvStream.pipe(fs.createWriteStream(file));

    return {
        writer: (messages) => messages.forEach((message) => csvStream.write(stringifyMessageAttributes(message))),
        end: () => csvStream.end(),
    };
}

function logQueues(queues) {
    queues.forEach((queue) => console.log(queue));
}

function logDescribe(queue, data) {
    console.log(`Queue: ${queue}`);
    console.log(`ApproximateNumberOfMessages: ${data.Attributes.ApproximateNumberOfMessages}`);
    console.log(`ApproximateNumberOfMessagesDelayed: ${data.Attributes.ApproximateNumberOfMessagesDelayed}`);
    console.log(`ApproximateNumberOfMessagesNotVisible: ${data.Attributes.ApproximateNumberOfMessagesNotVisible}`);
}

function logReceive(receive, deleteFromQueue, options) {
    if (!options.quiet) {
        console.log(`${receive.receiveCount} messages received from queue`);

        if (options.filter || options.transform) {
            console.log(`${receive.receiveCount - receive.filteredCount} messages ignored by filter/transform`);
        }

        console.log(`${receive.writeCount} messages written to file`);

        if (deleteFromQueue) {
            console.log(`${receive.deleteCount} messages deleted from queue`);
            console.log(`${receive.receiveCount - receive.deleteCount} messages failed to delete from queue`);
        }
    }
}

function logModify(modify, deleteFromQueue, options) {
    if (!options.quiet) {
        console.log(`${modify.readCount} messages read from file`);

        if (options.filter || options.transform) {
            console.log(`${modify.readCount - modify.filteredCount} messages ignored by filter/transform`);
        }

        console.log(`${modify.modifiedCount} messages ${deleteFromQueue ? 'deleted from' : `sent to`} queue`);
        console.log(
            `${modify.readCount - modify.modifiedCount} messages failed to ${
                deleteFromQueue ? 'delete from' : `send to`
            } queue`
        );
    }
}

function createMessageProcessor(options) {
    if (!options.filter && !options.transform) {
        return undefined;
    }

    const filter = options.filter ? vm.compileFunction(`return ${options.filter}`, ['message']) : () => true;
    const transform = options.transform
        ? vm.compileFunction(`${options.transform}; return message;`, ['message'])
        : (message) => message;

    return (message) => {
        if (!filter(message)) {
            return null;
        }

        return transform(message);
    };
}

async function sqsListQueuesAction(sqs) {
    const queues = await listQueues(sqs);
    logQueues(queues);
}

async function sqsDescribeQueueAction(sqs, options) {
    const queue = options.describe;

    const data = await describeQueue(queue, sqs);
    logDescribe(queue, data);
}

async function sqsReceiveAction(sqs, options) {
    const receive = new SqsReceive(sqs, {
        messageProcessor: createMessageProcessor(options),
        limit: options.limit,
        timeout: options.timeout,
    });

    const queue = options.list || options.extract;
    const deleteFromQueue = !!options.extract;

    const stream = createCsvWriteStream(options.file);

    try {
        await receive.receiveMessages(queue, stream.writer, deleteFromQueue);
        logReceive(receive, deleteFromQueue, options);
    } catch (error) {
        logReceive(receive, deleteFromQueue, options);
        throw error;
    } finally {
        stream.end();
    }
}

function sqsModifyAction(sqs, options) {
    const modify = new SqsModify(sqs, { messageProcessor: createMessageProcessor(options) });

    const queue = options.load || options.delete;
    const deleteFromQueue = !!options.delete;

    return new Promise((resolve, reject) => {
        const csvStream = fs
            .createReadStream(options.file)
            .on('error', (error) => modify.awaitPromises(error))
            .pipe(csv.parse({ headers: true }))
            .on('error', (error) => modify.awaitPromises(error))
            .pipe(parseMessageAttributesTransform());

        modify
            .modifyMessages(queue, csvStream, deleteFromQueue)
            .then(() => {
                logModify(modify, deleteFromQueue, options);
                resolve();
            })
            .catch((error) => {
                logModify(modify, deleteFromQueue, options);
                reject(error);
            });
    });
}

module.exports = async (sqs, options) => {
    if (options.help) {
        console.log(fs.readFileSync('src/help.txt', 'UTF-8'));
    } else if (options.queues) {
        await sqsListQueuesAction(sqs);
    } else if (options.describe) {
        await sqsDescribeQueueAction(sqs, options);
    } else if (options.list || options.extract) {
        await sqsReceiveAction(sqs, options);
    } else if (options.load || options.delete) {
        await sqsModifyAction(sqs, options);
    } else {
        console.log('Error: Action not specified');
    }
};
