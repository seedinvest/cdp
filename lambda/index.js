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
var async = require('async');
var AWS = require('aws-sdk');
// csvtojson quickly and easily parses and formats our CSV files
var csv = require('csvtojson');
// the following are Segment libraries
var Analytics = require('analytics-node');
var Objects = require('objects-node');

var analytics = new Analytics(process.env.write_key);
var objects = new Objects(process.env.write_key);
var s3 = new AWS.S3();
var callNames = ['identify','track','page','screen','group','alias','object'];

exports.handler = function(event, context, callback) {
    // Read options from the event.
    //console.log("Reading options from event:\n", event);
    var srcBucket = event.Records[0].s3.bucket.name;
    // File name may have spaces or unicode non-ASCII characters.
    var srcFileName =
        decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    var nameParts = srcFileName.split('/');
    var callToMake = null;
    for (var i = 0; i < nameParts.length; i++) {
        if(callNames.includes(nameParts[i])) {
            callToMake = nameParts[i];
            break;
        }
    }
    if (callToMake == null) {
        var srcFileNameLastPart = nameParts[nameParts.length - 1];
        for (var i = 0; i < callNames.length; i++) {
            if (srcFileNameLastPart.startsWith(callNames[i])) {
                callToMake = callNames[i];
                break;
            }
        }
    }

    // Download the CSV from S3, transform, and upload to Segment.
    // More on async.waterfall here: https://caolan.github.io/async/docs.html#waterfall
    async.waterfall([
            function download(next) {
                // Download the CSV from S3 into a buffer.
                //console.log("download");
                s3.getObject({
                        Bucket: srcBucket,
                        Key: srcFileName
                    },
                    next);
            },
            function transform(response, next) {
                //console.log("transform 1");
                var csvString = response.Body.toString();
                csv({
                    colParser: {
                        // Timestamp columns
                        "traits.created_at": function (item) {
                            return new Date(item);
                        },
                        "traits.modified_at": function (item) {
                            return new Date(item);
                        },
                        "traits.date_joined": function (item) {
                            return new Date(item);
                        },
                        "traits.last_login": function (item) {
                            return new Date(item);
                        },
                        "traits.user_request_to_be_forgotten_expiration_date": function (item) {
                            return new Date(item);
                        },
                        "traits.user_requested_to_be_forgotten_date": function (item) {
                            return new Date(item);
                        },
                        // Boolean columns
                        "traits.email_confirmed": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited_via_entity": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited_via_income": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited_via_finra_license": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited_via_trust": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        "traits.is_accredited_via_networth": function (item) {
                            return Boolean(item == "true" || item == "True");
                        },
                        // Number columns
                        "traits.country_of_citizenship_id": function (item) {
                            return Number(item);
                        },
                        "traits.user_identity_id": function (item) {
                            return Number(item);
                        },
                        "traits.user_id": function (item) {
                            return Number(item);
                        },
                        "traits.user_accreditation_status_id": function (item) {
                            return Number(item);
                        },
                        "traits.user_association_information_id": function (item) {
                            return Number(item);
                        },
                        // Structured columns
                        "traits.applications": function (item) {
                            try {
                                return JSON.parse(item);
                            } catch (err) {
                                console.log("Parse traits.applications Error: ", err);
                                console.log("Failed Item: ", item)
                                return null;
                            }
                        },
                        "traits.prequalifications": function (item) {
                            try {
                                return JSON.parse(item);
                            } catch (err) {
                                console.log("Parse traits.prequalifications Error: ", err);
                                console.log("Failed Item: ", item)
                                return null;
                            }
                        },
                    }
                })
                    .fromString(csvString)
                    .then((formattedResults) => {
                        next(null, formattedResults);
                    })
            },
            function upload(formattedResults, next) {
                //console.log("upload");
                //console.log(formattedResults);
                if ('identify' === callToMake) {
                    formattedResults.map(function (identifyObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/identify/
                        analytics.identify(identifyObject);
                    });
                } else if ('track' === callToMake) {
                    formattedResults.map(function (trackObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/track/
                        analytics.track(trackObject);
                    });
                } else if ('page' === callToMake) {
                    formattedResults.map(function (pageObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/page/
                        analytics.page(pageObject);
                    });
                } else if ('screen' === callToMake) {
                    formattedResults.map(function (screenObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/screen/
                        analytics.screen(screenObject);
                    });
                } else if ('group' === callToMake) {
                    formattedResults.map(function (groupObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/group/
                        analytics.group(groupObject);
                    });
                } else if ('alias' === callToMake) {
                    formattedResults.map(function (aliasObject) {
                        // More in the docs here: https://segment.com/docs/connections/spec/alias/
                        analytics.alias(aliasObject);
                    });
                } else if ('object' === callToMake) {
                    // Who doesn't love a variable named objectObject?! :(
                    formattedResults.map(function (objectObject) {
                        // The Object API accepts a different format than the other APIs
                        // More in the docs here: https://github.com/segmentio/objects-node
                        // First, we get our collection name
                        var objectCollection = srcFileNameLastPart.split("_")[1];
                        // Then, we get and delete our objectId from the object we pass into Segment
                        var objectId = objectObject.id;
                        delete objectObject.id;
                        console.log("objectCollection: ", objectCollection);
                        console.log("objectId: ", objectId);
                        console.log("objectObject: ", objectObject);
                        objects.set(objectCollection, objectId, objectObject);
                    });
                } else {
                    console.log("ERROR! No call type specified! Your CSV file in S3 should start with 'identify_', 'track_', 'page_', 'screen_', 'group_', 'alias_' or 'object_<collection>'");
                    console.log("srcBucket:", srcBucket, ", srcFileName:", srcFileName, ", nameParts:", nameParts, ", callToMake:", callToMake);
                    throw new Error;
                }
                // Now we make sure that all of our queued actions are flushed before moving on
                if (srcFileName.startsWith('object_')) {
                    var objectCollection = srcFileName.split("_")[1];
                    objects.flush(objectCollection, function (err, batch) {
                        next(err, "Done");
                    });
                } else {
                    analytics.flush(function (err, batch) {
                        next(err, "Done");
                    });
                }
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
