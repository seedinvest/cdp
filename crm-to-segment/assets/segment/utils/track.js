const Util = require('./index');

const segmentTrackObj = {
  userId: null,
  event: '',
  properties: {},
};

const segmentTrackProperties = {
  company_name: '',
  deal_id: '',
  deal_name: '',
  offering_type: '',
};

const setTrackProperties = (message) => {
  const props = { ... segmentTrackProperties };

  if (Util.isObject(message.target)) {
    props.company_name = message.target.company_name;
    props.deal_id = message.target.id;
    props.deal_name = message.target.name;
    props.offering_type = message.target.offering_type;
  }

  if (Util.isObject(message.details)) {
    props.amount = message.details.amount;
  }

  return props;
}

const getSegmentTrack = (message) => {
  const track = { ... segmentTrackObj };

  track.userId = message.user_id;
  track.event = message.event_type;
  track.properties = setTrackProperties(message);

  return track;
};

module.exports = {
  getSegment: getSegmentTrack,
};

