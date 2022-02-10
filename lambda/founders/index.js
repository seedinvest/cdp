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
  const srcFileName =
    decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
  console.log(srcFileName)

  async.waterfall([
    function download(next) {
      s3.getObject({
        Bucket: srcBucket,
        Key: srcFileName
      }, next);
    },
    function transform(response, next) {
      console.log("transform 1");
      const csvString = response.Body.toString();
      csv({
        colParser: {
          "userId": function (item) {
            return Number(item);
          },
        }
      })
      .fromString(csvString)
      // .subscribe((jsonObj) => {
      //   jsonObj['groupId'] = jsonObj.traits.groupId;
      //   delete jsonObj.traits.groupId;
      // })
      .then((formattedResults) => {
        next(null, formattedResults);
      });
    },
    function upload(formattedResults, next) {
      console.log("upload");
      console.log(formattedResults);
      formattedResults.map(function (identifyObject) {
        // More in the docs here: https://segment.com/docs/connections/spec/identify/
        analytics.identify(identifyObject);
      });

      analytics.flush(function (err, batch) {
        next(err, "Done");
      });

      // if ('identify' === callToMake) {
      //     formattedResults.map(function (identifyObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/identify/
      //         analytics.identify(identifyObject);
      //     });
      // } else if ('track' === callToMake) {
      //     formattedResults.map(function (trackObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/track/
      //         analytics.track(trackObject);
      //     });
      // } else if ('page' === callToMake) {
      //     formattedResults.map(function (pageObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/page/
      //         analytics.page(pageObject);
      //     });
      // } else if ('screen' === callToMake) {
      //     formattedResults.map(function (screenObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/screen/
      //         analytics.screen(screenObject);
      //     });
      // } else if ('group' === callToMake) {
      //     formattedResults.map(function (groupObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/group/
      //         analytics.group(groupObject);
      //     });
      // } else if ('alias' === callToMake) {
      //     formattedResults.map(function (aliasObject) {
      //         // More in the docs here: https://segment.com/docs/connections/spec/alias/
      //         analytics.alias(aliasObject);
      //     });
      // } else if ('object' === callToMake) {
      //     // Who doesn't love a variable named objectObject?! :(
      //     formattedResults.map(function (objectObject) {
      //         // The Object API accepts a different format than the other APIs
      //         // More in the docs here: https://github.com/segmentio/objects-node
      //         // First, we get our collection name
      //         var objectCollection = srcFileNameLastPart.split("_")[1];
      //         // Then, we get and delete our objectId from the object we pass into Segment
      //         var objectId = objectObject.id;
      //         delete objectObject.id;
      //         console.log("objectCollection: ", objectCollection);
      //         console.log("objectId: ", objectId);
      //         console.log("objectObject: ", objectObject);
      //         objects.set(objectCollection, objectId, objectObject);
      //     });
      // } else {
      //     console.log("ERROR! No call type specified! Your CSV file in S3 should start with 'identify_', 'track_', 'page_', 'screen_', 'group_', 'alias_' or 'object_<collection>'");
      //     console.log("srcBucket:", srcBucket, ", srcFileName:", srcFileName, ", nameParts:", nameParts, ", callToMake:", callToMake);
      //     throw new Error;
      // }
      // // Now we make sure that all of our queued actions are flushed before moving on
      // if (srcFileName.startsWith('object_')) {
      //     var objectCollection = srcFileName.split("_")[1];
      //     objects.flush(objectCollection, function (err, batch) {
      //         next(err, "Done");
      //     });
      // } else {
      //     analytics.flush(function (err, batch) {
      //         next(err, "Done");
      //     });
      // }
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
