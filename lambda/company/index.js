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
const async = require('async');
const AWS = require('aws-sdk');
const csv = require('csvtojson');
const s3 = new AWS.S3();

// the following are Segment libraries
const Analytics = require('analytics-node');
const analytics = new Analytics(process.env.write_key);

exports.handler = function (event, context, callback) {
  const srcBucket = event.Records[0].s3.bucket.name;
  // File name may have spaces or unicode non-ASCII characters.
  const srcFileName = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));

  async.waterfall([
    function download(next) {
      s3.getObject({
        Bucket: srcBucket,
        Key: srcFileName
      }, next);
    },
    function transform(response, next) {
      console.log('Transforming');
      const csvString = response.Body.toString();
      csv()
        .fromString(csvString)
        .then((formattedResults) => {
          next(null, formattedResults);
        });
    },
    function upload(formattedResults, next) {
      console.log('Uploading', formattedResults);
      formattedResults.map(function (groupObject) {
        // More in the docs here: https://segment.com/docs/connections/spec/group/
        analytics.group(groupObject);
      });

      analytics.flush(function (err, batch) {
        next(err, "Done");
      });
    }
  ], function (err) {
    // Some pretty basic error handling
    if (err) {
      console.error(
        'Unable to download ' + srcBucket + '/' + srcFileName +
        ' and upload to Segment' +
        ' due to an error: ' + err
      );
    } else {
      console.log(
        'Successfully downloaded ' + srcBucket + '/' + srcFileName +
        ' and uploaded to Segment!'
      );
    }
    callback(null, "Success!");
  }
  );
};
