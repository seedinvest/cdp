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
  obj.properties = {};
  obj.event_object = JSON.parse(obj.event_object);
  obj.event_details = JSON.parse(obj.event_details);
  
  // parse the deal object 
  if (obj.event_object.type && obj.event_object.type === 'deal') {
    obj.properties.deal_id = obj.event_object.id;
    obj.properties.deal_name = obj.event_object.name;
    obj.properties.company_name = obj.event_object.company_name;
    obj.properties.offering_type = obj.event_object.offering_type;
  }
  
  // parse the amount
  if (obj.event_details.amount) {
    obj.properties.amount = obj.event_details.amount;
  }
  
  const ts = new Date(obj.timestamp);

  obj.messageId = `db-track-${obj.userId}-${ts.getTime()}`;
  obj.timestamp = ts;
  
  delete obj.event_details;
  delete obj.event_object;
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
      analytics.track(event);
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
