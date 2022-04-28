'use strict';

const Analytics = require('analytics-node');
const SegmentIdentify = require('./utils/identify')
const SegmentTrack = require('./utils/track');
const Util = require('./utils');

exports.handler = (event, context, callback) => {
  const analytics = new Analytics(process.env.write_key, { flushAt: 1 });
  const isDryRun = process.env.is_dry_run == 'true' ? true : false;
  const isDebug = process.env.is_debug == 'true' ? true : false;
  const dryRunPrefix = isDryRun ? 'DRY RUN (NOT SENDING TO SEGMENT):' : '';

  try  {
    for (const record of event.Records) {
      const message = Util.getMessageFromRecord(record);

      if (isDebug) {
        console.log('message: %j', message);
      }

      let payload = {};
      switch (message.event) {
        case 'USER_TRACK':
          payload = SegmentTrack.getSegment(message);
          console.log(`${dryRunPrefix} user_track: %j`, payload);
          if (!isDryRun) {
            analytics.track(payload);
          }
        break;

        case 'USER_IDENTIFY':
          payload = SegmentIdentify.getSegment(message);
          console.log(`${dryRunPrefix} user_identify: %j`, SegmentIdentify.getSegment(message));
          if (!isDryRun) {
            analytics.identify(SegmentIdentify.getSegment(message));
          }

        break;

        default:
          console.error(`Unknown event type for event: ${event}`);
          callback(Error(`Unknown event type for event: ${message.event}`));
      }
    }

    if (!isDryRun) {
      analytics.flush((err, batch) => {
        if (err) {
          console.error(`Error flushing: ${err}`);
          callback(e);
        } else {
          console.log('Segment flushed successful');
          callback(null, 'Segment flushed successful');
        }
      });
    } else {
      callback(null, 'Dry run complete.');
    }

  } catch (e) {
    callback(e);
  }
};