#!/usr/bin/env node

/* eslint-disable no-console */
const AWS = require('aws-sdk');

const sqsUtility = require('./sqsUtility');
const parseOptions = require('./parseOptions');

const options = parseOptions();

AWS.config.update({
    region: options.region || 'eu-west-2',
    endpoint: options.endpoint,
});
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

sqsUtility(sqs, options).catch((error) => console.error(error));
