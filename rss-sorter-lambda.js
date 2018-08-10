'use strict';

require('dotenv').config()
const { Client } = require('pg');
let Parser = require('rss-parser');

// The RSS Parser turns an RSS feed into an array of objects.
let parser = new Parser();

// Clients will use environment variables for connection information
// NOTE: Be sure to set up env variables in the lambda.
// For testing, a .env file is required that looks like this:
// PGHOST=<DB ENDPOINT>.us-east-2.rds.amazonaws.com
// PGUSER=<USER>
// PGPASS=<PASSSWORD>
// PGPORT=5432
// PGDATABASE="rssSorter"
const client = new Client();
try {
  client.connect();
}
catch(e) {
  console.log(`DB connection error: ${e}`);
}

// Parses rss feed into an array of objects
async function incomingFeed(feedURL) {  
  const t0 = process.hrtime();
  console.log('Parse incoming feed started.');
  try {
    let feedItemsPromise = parser.parseURL(feedURL);
    let feedItems = await feedItemsPromise;
    console.log('Parse incoming feed contains ' + feedItems.items.length + ' items and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
    return feedItems
  }
  catch(e) {
    console.log(`Incoming feed error: ${e}`);
  }
}

// Grabs guids for last 100 items for duplication checking
async function last100Items() {
  const t0 = process.hrtime();
  console.log('Get last 100 guids started.');
  try {
    const guidArr = [];
    let last100promise = client.query('SELECT guid FROM "feedDetails" ORDER BY "pubDate" DESC LIMIT 100;');
    let last100 = await last100promise;
    last100.rows.forEach(guid => {
      guidArr.push(guid);
    })
    const t1 = process.hrtime(t0);
    console.log('Get last 100 guids contains ' + last100.rows.length + ' guids and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
    //Map objects to an array
    let last100Arr = guidArr.map(function(item) {
      return item['guid'];
    });
    console.log(last100Arr);
    return last100Arr;
  }
  catch(e) {
    console.log(`DB read error: ${e}`);
  }
}

// Eliminates dupes and other irrelevant rows from the feed. 
function filterArray(newFeed, oldGuids) {
  const t0 = process.hrtime();
  const startCount = newFeed.items.length;
  console.log('filterArray function started with ' + startCount + ' items.');
  let itemsArr = [];
  //Filter feed for dupes and spam rows
  newFeed.items.forEach((item) => {
    if ((oldGuids.includes(item.guid) === false) && 
       (item.content.includes('NJ TRANSIT Printable Timetables') === false) &&
       (item.content.includes('Service Adjustments Required to Advance Positive Train Control (PTC)') === false)) {
      let itemToAdd = ` ('${item.title}', '${item.content}', '${item.link}', '${item.pubDate}', '${item.guid}', 1)`;
      itemsArr.push(itemToAdd);
    }
  })
  //Mark booleans as true that match conditions, then increases priority if any conditions are met.
  itemsArr.forEach((item) => {
    //TODO ERROR HERE cannot mutate object this way
    if (item.includes('cancel')) { item.cancelTF = true };
    if (item.includes('delay')) { item.delayTF = true };
    if (item.includes('change' || 'replace')) { item.changeTF = true };
    if (item.cancelTF || item.delayTF || item.changeTF) {item.priority = 'High'};
  })
  const endCount = itemsArr.length;
  let items = itemsArr.join();
  items = items + ';';
  console.log('filterArray function removed ' + (startCount - endCount) + ' items and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
  return items;
}

async function writeItemsToDb(writeItems, startTime) {
  const t0 = process.hrtime();
  console.log('Write feed to DB started.')
  try {
    await client.query(`INSERT INTO "feedDetails" ("title", "description", "link", "pubDate", "guid", "feedID", "cancelTF", "delayTF", "changeTF", "priority")
      VALUES ${writeItems};`)
    .then((res) => {
      client.end();
      console.log('writeItemsToDb wrote ' + res.rowCount  + ' rows and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
      console.log('RSS Sorter Lambda took ' + ((process.hrtime(startTime)[1]) / 1e9) + ' seconds to run.')
    })
  }
  catch(e) {
    console.log(`DB write error: ${e}`);
  }
}

(async() => {
  const overallT0 = process.hrtime();
  let RSSFeedURL = 'https://www.njtransit.com/rss/RailAdvisories_feed.xml';
  let feedPromise = incomingFeed(RSSFeedURL);
  let guidArrayPromise = last100Items();
  await Promise.all([feedPromise, guidArrayPromise]).then((values) => {
    let feed = values[0];
    let oldGuidArray = values[1]; 
    const writeString = filterArray(feed, oldGuidArray);
    if (writeString.length > 1) {
      writeItemsToDb(writeString, overallT0);
    } else {
      console.log('writeItemsToDb wrote 0 rows (All items were duplicates or matched keywords).');
      console.log('RSS Sorter Lambda took ' + ((process.hrtime(overallT0)[1]) / 1e9) + ' seconds to run.')
    }
  })
})();

