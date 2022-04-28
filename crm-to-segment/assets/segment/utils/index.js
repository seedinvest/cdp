const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');

const getMessageFromRecord = (record) => {
  const body = JSON.parse(record.body);
  return JSON.parse(body.Message);
};

const isObject = (obj) => {
  return typeof obj === 'object' && obj !== null;
}

module.exports = {
  getMessageFromRecord: getMessageFromRecord,
  isObject: isObject,
};
