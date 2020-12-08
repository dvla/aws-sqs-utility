# SQS Utility

Command-line utility that provides the ability to read, write and transform messages on a AWS SQS queue. The following queue actions are available:

* **List** - Copy messages from a SQS queue into a CSV file or onto another SQS queue. The messages remain on the queue.

* **Extract** - Read messages from a SQS queue into a CSV file or onto another SQS queue. The messages are removed from the queue.

* **Load** - Write messages from a CSV file onto a SQS queue.

* **Delete** - Delete messages specified in a CSV file from a SQS queue.

For each queue action a **filter** and/or **transform** can be specified to limit or alter the messages being read/written by the action.

The following actions are also available to provide SQS information:

* **Describe** - Return information on an SQS queue state

* **Queues** - List the SQS queues in an AWS account

## Installation

To install the SQS Utility as a command-line tool run (requires sudo permissions, omit sudo on Windows):

`sudo npm install -g`

To install the SQS Utility locally run:

`npm install`

## AWS Credentials

To utilise SQS Utility you must have valid AWS CLI credentials configured:

https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

## Usage

If SQS Utility is installed as a command-line tool run:

`sqs-utility <action>`

If SQS Utility is installed locally run:

`node . <options>`

The options must include one of the following actions:

* `--list <queue url>` - list messages from queue
* `--extract <queue url>` - extract messages from queue
* `--load <queue url>` - load messages onto queue
* `--delete <queue url>` - delete messages from queue
* `--describe <queue url>` - information of the queue state
* `--queues` - list available queues
* `--help` - display help

For all actions (except describe/queues) one of the following targets must be specified:

* `--file <file>` - file to read/write messages (CSV)

The following configuration options are available:

* `--filter <javascript filter>` - apply message filter (see below)
* `--transform <javascript transform>` - apply message transform (see below)
* `--limit <limit>` - maximum number of messages retrieved by list/extract, default is 1000
* `--timeout <seconds>` - maximum seconds to list/extract messages, default is 30
* `--region <aws region>` - override default AWS region
* `--endpoint <aws endpoint url>` - override default AWS endpoint URL
* `--quiet` - suppress output

## Examples

Describe the current queue state:

`sqs-utility --describe https://sqs.eu-west-2.amazonaws.com/12345/SourceQueue`

List messages from a queue into a CSV file:

`sqs-utility --list https://sqs.eu-west-2.amazonaws.com/12345/SourceQueue --file messages.csv`

Extract messages from a queue into a CSV file:

`sqs-utility --extract https://sqs.eu-west-2.amazonaws.com/12345/SourceQueue --file messages.csv`

Load messages from a CSV file onto a queue:

`sqs-utility --load https://sqs.eu-west-2.amazonaws.com/12345/TargetQueue --file messages.csv`

## Filters

The `--filter` configuration option allows messages to be filtered using a Javascript expression. This limits which messages listed/extracted from or loaded to the queue.

For example, the following filter will only list/extract/load messages where the message body contains `'Some Value'`:

`--filter "message.Body.contains('Some Value')"`

## Transforms

The `--transform` configuration option allows messages to be transformed using Javascript commands before being listed/extracted from or loaded to the queue.

For example, the following transform will update the message body by replacing occurrences of `'Some Value'` with `Another Value`, and by adding a custom field:

`--transform "message.Body = message.Body.replace('Some Value', 'Another Value); message.Custom = 'Custom Value';"`

Note that _only_ transforms to the body and message attributes will be sent to the queue when using the load action.

## Tests

To execute the unit tests run:

`npm run test`.

The system tests require [AWS Localstack](https://github.com/localstack/localstack) to be installed and started. The system tests can then be run using:

`npm run system-test`

## License

The MIT License (MIT)

Copyright (c) 2020 DVLA

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
