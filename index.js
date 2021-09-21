import AWS from 'aws-sdk';
import fs from 'fs';

// config
const SEARCH_START = '2021-08-26 00:00:00';

const SEARCH_END   = '2021-09-21 23:59:59';

const LIMIT = 10000;

const AWS_REGION = 'ap-northeast-1';

const FILE_PATH = 'INPUT OUTPUT FILE PATH';

const LOG_GROUPS = [
  'INPUT LOG GROUP NAME',
];

AWS.config.update({region: AWS_REGION});

AWS.config.getCredentials(function(err) {
  if (err) console.log(err.stack);
  // credentials not loaded
  else {
    console.log("Access key:", AWS.config.credentials.accessKeyId);
  }
});

const cloudwatchlogs = new AWS.CloudWatchLogs();

/**
 * @param {[string, (string | undefined)][]|string[]} arr
 */
const arrToCSV = (arr) => arr
  .map(row => row.map(str => '"' + (str ? str.replace(/"/g, '""') : '') + '"')
  )
  .map(row => row.join(','))
  .join('\n');

/**
 * @param {number} startTime
 * @param {number} endTime
 */
const requestLog = (startTime, endTime) => {
  return new Promise((resolve, reject) => {
    const params = {
      startTime: startTime,
      endTime: endTime,
      queryString: 'fields @timestamp, @message\n' +
        '| sort @timestamp asc',
      limit: LIMIT,
      logGroupNames: LOG_GROUPS
    };
    cloudwatchlogs.startQuery(params, function(err, data) {
      if (err) {
        reject(err);
      }
      else {
        resolve(data['queryId']);
      }
    });
  });
}

const receiveLog = (queryId) => {
  return new Promise((resolve, reject) => {
    const params = {
      queryId
    };
    cloudwatchlogs.getQueryResults(params, function(err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        reject(err);
      }
      else {
        console.log(data['status']);
        if (data['results'].length < LIMIT && data['status'] != 'Complete') {
          reject("on ready");
          return;
        }

        const results = data['results'].map((item) => {
          const timestamp = item[0]['value'];
          return [
            String(new Date(timestamp).getTime()),
            item[1]['value']
          ];
        })
        const csv = arrToCSV(results)
        fs.appendFileSync(FILE_PATH, csv);

        console.log("result length");
        console.log(results.length);
        if (results.length == 0) {
          resolve(0);
        } else {
          console.log(results[results.length-1][0]);
          resolve(Number(results[results.length-1][0]) + 1);
        }
      }
    });
  });
}

/**
 * @param {*} queryId
 * @param {number} retryCount
 */
const retryReceive = async (queryId, retryCount=0) => {
  if (retryCount == 5) {
    throw 'Parameter is not a number!';
  }
  try {
    return await receiveLog(queryId);
  } catch (e) {
    await sleep(20000);
    return await retryReceive(queryId, retryCount++);
  }
};

/**
 * @param {number} time
 */
const sleep = (time) => new Promise((resolve, reject) => {
  setTimeout(() => {
    resolve();
  }, time);
});

(async () => {

  let   startTime =   new Date(SEARCH_START).getTime();
  const endTime   = new Date(SEARCH_END).getTime();

  while (startTime <= endTime) {

    let searchEndTime   = startTime + ((new Date().getTimezoneOffset() + (24 * 60)) * 60 * 1000);

    console.log(`start ${startTime} - ${searchEndTime}`);

    let queryId = await requestLog(startTime, searchEndTime);
    console.log(queryId);
    startTime = await retryReceive(queryId);

    if (startTime <= searchEndTime) {
      startTime  = searchEndTime + 1;
    }
  }

  console.log('finished!');
})();

