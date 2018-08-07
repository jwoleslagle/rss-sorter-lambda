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
// PGUSER=rss_sorter_admin
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
    console.log('Parse incoming feed finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
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
    return last100;
  }
  catch(e) {
    console.log(`DB read error: ${e}`);
  }

}

// Eliminates dupes and other irrelevant rows from the feed. 
function filterArray(newFeed, oldGuids) {
  const t0 = process.hrtime();
  const startCount = newFeed.length;
  console.log('Feed dupe filter started with ' + startCount + ' items.');
  let itemsArr = [];
  newFeed.items.forEach((item) => {
    if ((oldGuids.rows.includes(item.guid) === false) && 
       (item.content.includes('NJ TRANSIT Printable Timetables') === false) &&
       (item.content.includes('Service Adjustments Required to Advance Positive Train Control (PTC)') === false)) {
      const itemToAdd = ` ('${item.title}', '${item.content}', '${item.link}', '${item.pubDate}', '${item.guid}', 1)`;
      itemsArr.push(itemToAdd);
    }
  })
  const endCount = items.length;
  let items = itemsArr.join();
  items = items + ';';
  console.log('filterArray function removed ' + (startCount - endcount) + ' items and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
  return items;
}

async function writeItemsToDb(writeItems) {
  const t0 = process.hrtime();
  console.log('Write feed to DB started.')
  try {
    await client.query(`INSERT INTO "feedDetails" ("title", "description", "link", "pubDate", "guid", "feedID")
      VALUES ${writeItems};`);
    console.log('Write feed to DB finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
  }
  catch(e) {
    console.log(`DB write error: ${e}`);
  }
  
}

(async() => {
  let RSSFeedURL = 'https://www.njtransit.com/rss/RailAdvisories_feed.xml';
  let feedPromise = incomingFeed(RSSFeedURL);
  let guidPromise = last100Items();
  let feed = await feedPromise;
  let guidArray = await guidPromise;
  //const writeString = filterArray(feed, guidArray);
  //writeItemsToDb(writeString);
  client.end();
})();

