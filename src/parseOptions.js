/* eslint-disable no-console */
const commandLineArgs = require('command-line-args');

function isDefined(arg) {
    return arg !== undefined;
}

function isUndefined(arg) {
    return arg === undefined;
}

// eslint-disable-next-line consistent-return
module.exports = () => {
    let options;

    try {
        options = commandLineArgs([
            // actions
            { name: 'queues', alias: 'q', type: Boolean },
            { name: 'describe', type: String },
            { name: 'list', alias: 'i', type: String },
            { name: 'extract', alias: 'e', type: String },
            { name: 'load', alias: 'l', type: String },
            { name: 'delete', alias: 'd', type: String },
            { name: 'help', alias: 'h', type: Boolean },
            // output
            { name: 'file', alias: 'f', type: String },
            { name: 'target', alias: 't', type: String },
            // options
            { name: 'filter', type: String },
            { name: 'transform', type: String },
            { name: 'limit', type: Number, defaultValue: 1000 },
            { name: 'timeout', type: Number, defaultValue: 30 },
            { name: 'region', type: String },
            { name: 'endpoint', type: String },
            { name: 'quiet', type: Boolean },
        ]);

        const actions = [
            isDefined(options.queues) && 'queues',
            isDefined(options.describe) && 'describe',
            isDefined(options.list) && 'list',
            isDefined(options.extract) && 'extract',
            isDefined(options.load) && 'load',
            isDefined(options.delete) && 'delete',
            isDefined(options.help) && 'help',
        ].filter((action) => !!action);

        if (actions.length === 0) {
            console.error('Error: No action specified');
            process.exit(1);
        }

        if (actions.length > 1) {
            console.error(`Error: Multiple actions specified [${actions.join(', ')}]`);
            process.exit(1);
        }

        if (
            options.describe === null ||
            options.list === null ||
            options.extract === null ||
            options.load === null ||
            options.delete === null
        ) {
            console.error(`Error: No queue specified for ${actions[0]} action`);
            process.exit(1);
        }

        if (isDefined(options.file) && isDefined(options.target)) {
            console.error('Error: Cannot specify both file and target');
            process.exit(1);
        }

        if (options.queues && (isDefined(options.file) || isDefined(options.target))) {
            console.error('Error: File/target not allowed for queues action');
            process.exit(1);
        }

        if (options.describe && (isDefined(options.file) || isDefined(options.target))) {
            console.error('Error: File/target not allowed for describe action');
            process.exit(1);
        }

        if (options.list && isUndefined(options.file) && isUndefined(options.target)) {
            console.error('Error: File/target required for list action');
            process.exit(1);
        }

        if (options.extract && isUndefined(options.file) && isUndefined(options.target)) {
            console.error('Error: File/target required for extract action');
            process.exit(1);
        }

        if (options.load && isUndefined(options.file)) {
            console.error('Error: File required for load action');
            process.exit(1);
        }

        if (options.delete && isUndefined(options.file)) {
            console.error('Error: File required for delete action');
            process.exit(1);
        }

        if (Number.isNaN(options.limit) || options.limit <= 0) {
            console.error('Error: Invalid limit');
            process.exit(1);
        }

        return options;
    } catch (err) {
        console.error(err.message);
        process.exit(1);
    }
};
