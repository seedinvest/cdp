/*
 * Copyright (c) 2021 Circle Internet Financial Trading Company Limited.
 * All rights reserved.
 *
 * Circle Internet Financial Trading Company Limited CONFIDENTIAL
 *
 * This repository includes unpublished proprietary source code of Circle Internet
 * Financial Trading Company Limited, Inc. The copyright notice above does not
 * evidence any actual or intended publication of such source code. Disclosure
 * of this source code or any related proprietary information is strictly
 * prohibited without the express written permission of Circle Internet Financial
 * Trading Company Limited.
 */

/**
 * A lambda for to sync CSV files on S3 to Segment.
 * Load the email preferences and send identify events to Segment.
 */

// dependencies
const AWS = require('aws-sdk');
const csv = require('csvtojson');
const s3 = new AWS.S3();

// the following are Segment libraries
const Analytics = require('analytics-node');
const analytics = new Analytics(process.env.write_key);

function transform(obj) {
  
  obj.messageId = `db-identify-email-${obj.userId}`;
  
  const ts = new Date(obj.timestamp);
  obj.timestamp = ts;

  obj.traits = {};
  const subscribed = obj.subscribed.split(' ');
  subscribed.forEach(topic => obj.traits[`${topic}`] = 'true')
  delete obj.subscribed;

  obj.traits.email = obj.email;
  delete obj.email;
  return obj;
};

exports.handler = function (event, context, callback) {
  const srcBucket = event.Records[0].s3.bucket.name;
  const srcFileName = event.Records[0].s3.object.key;

  const params = {
    Bucket: srcBucket,
    Key: srcFileName,
  };

  csv()
    .fromStream(s3.getObject(params).createReadStream())
    .subscribe((json) => {
      const event = transform(json);
      console.log(event);
      analytics.identify(event);
    }, (err) => {
      console.error(err);
    }, async () => {});
};
