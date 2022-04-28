const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const Util = require('./index');

const segmentIdentifyObj = {
  userId: null,
  traits: {
    created_at: '',
    email_confirmed: false,
    email: '',
    first_name: '',
    is_accredited: false,
    join_date: '',
    last_name: '',
  },
};

const getSegmentIdentify = (message) => {
  dayjs.extend(utc);
  const identify = { ... segmentIdentifyObj };

  identify.userId = message.user_id;
  identify.traits = {
    first_name: message.traits.first_name,
    last_name: message.traits.last_name,
    email: message.traits.email,
    email_confirmed: message.traits.email_confirmed,
  };

  if (Util.isObject(message.traits.accreditation_status)) {
    identify.traits.is_accredited = message.traits.accreditation_status.is_accredited || false;
  }

  if (message.traits.join_date) {
    const d = dayjs(message.traits.join_date);
    if (d.isValid()) {
      identify.traits.created_at = d.toISOString();
      identify.traits.date_joined = d.toISOString();
    }
  }

  return identify;
}

module.exports = {
  getSegment: getSegmentIdentify,
};
