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
 *
 * See design doc:
 * https://docs.google.com/document/d/1yBsv0ECwitoieDWH8aB5Pt1mc8G6-SPJuok3s_W_Ox8/
 */

// dependencies
// const async = require('async');
const AWS = require('aws-sdk');
const csv = require('csvtojson');
const s3 = new AWS.S3();

// the following are Segment libraries
const Analytics = require('analytics-node');
const analytics = new Analytics(process.env.write_key);

function transform(obj) {
  if (obj.timestamp) {
    obj.timestamp = new Date(obj.timestamp);
  }
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
      console.log(transform(json));
      analytics.track(transform(json));
    }, (err) => {
      console.error(err);
    }, async () => {
      // await analytics.flush((err, batch) => {
      //   if (!err) {
      //     console.log(`Successfully downloaded ${srcBucket}/${srcFileName} and uploaded to Segment!`);
      //     callback(null, "Success!");
      //   } else {
      //     console.error(`Unable to download ${srcBucket}/${srcFileName} and uploaded to Segment due to ${err}`);
      //   }
      // });
    });
};
