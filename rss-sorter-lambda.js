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

function lineLookup(desc) {
  let line = [];
  if (desc.includes('ACRL') || desc.includes('Atlantic City Rail Line')) { line.push('ACRL')};
  if (desc.includes('MOBO') || desc.includes('Montclair-Boonton Line')) { line.push('MOBO')};
  if (desc.includes('MBPJ') || desc.includes('Main / Bergen / Port Jervis Line') || desc.includes('Main/Bergen/Port Jervis Line')) { line.push('MBPJ')};
  if (desc.includes('M&E') || desc.includes('Morris and Essex Line') || desc.includes('Morris & Essex Line') || desc.includes('Gladstone Line') || desc.includes('Gladstone Branch')) { line.push('M&E') };
  if (desc.includes('NEC') || desc.includes('Northeast Corridor')) { line.push('NEC') };
  if (desc.includes('NJCL') || desc.includes('North Jersey Coast Line')) { line.push('NJCL') };
  if (desc.includes('PVL') || desc.includes('Pascack Valley Line')) { line.push('PVL') };
  if (desc.includes('RVL') || desc.includes('Raritan Valley Line')) { line.push('RVL') };
  if (line.length === 0) { line.push('')};
  return line;
}

function stationLookup(descr) {
  //TODO write this function
}

// Eliminates dupes and other irrelevant rows from the feed, then tags the entry with line, priority, and late / change / cancel booleans. 
function filterArray(newFeed, oldGuids) {
  const t0 = process.hrtime();
  const startCount = newFeed.items.length;
  console.log('filterArray function started with ' + startCount + ' items.');
  let itemsArr = [];
  //Filter feed for dupes and spam rows
  newFeed.items.forEach((item) => {
    item.cancelTF = false;
    item.delayTF = false;
    item.changeTF = false;
    item.priority = 'Low';
    item.feedID = 1;
    item.line = '';
    if ((oldGuids.includes(item.guid) === false) && 
       (item.content.includes('NJ TRANSIT Printable Timetables') === false) &&
       (item.content.includes('Service Adjustments Required to Advance Positive Train Control (PTC)') === false)) {
      //Mark booleans as true that match conditions, then increases priority if any conditions are met.
      let itemContent = item.content.toString();
      let itemContentToLower = itemContent.toLowerCase();
      if (itemContentToLower.includes('cancel')) { item.cancelTF = true };
      if (itemContentToLower.includes('late') || itemContentToLower.includes('delay')) { item.delayTF = true };
      if (itemContentToLower.includes('change') || itemContentToLower.includes('replace')) { item.changeTF = true };
      if (item.cancelTF || item.delayTF || item.changeTF) {item.priority = 'High'};
      item.line = lineLookup(itemContent);
      itemsArr.push(item);
    }
  })
  const endCount = itemsArr.length;
  console.log('filterArray function removed ' + (startCount - endCount) + ' items and finished in ' + ((process.hrtime(t0)[1]) / 1e9) + ' seconds.');
  return itemsArr;
}

//entryStringifier transforms the raw RSS object into a postgres insert-friendly string.
function entryStringifier(entriesArr) {
  let bigString = '';
  entriesArr.forEach((e) => {
    let littleString = `(
    '${e.title}',
    '${e.content},
    '${e.link}', 
    '${e.pubDate}',
    '${e.guid}',
     ${e.feedID},
     ${e.cancelTF}, 
     ${e.delayTF},
     ${e.changeTF},
    '${e.priority}',
    '${e.line ? `{ ${e.line} }` : `{}`}'
     ),`;
    bigString += littleString;
  });
  //remove the last comma and put a semicolon in its place
  const finalString = bigString.substr(0, bigString.length - 1) + ";";
  return finalString;
}

async function writeItemsToDb(writeArr, startTime) {
  const t0 = process.hrtime();
  console.log('Write feed to DB started.')
  const dbInputString = entryStringifier(writeArr);
  try {
    await client.query(`INSERT INTO "feedDetails" (title, content, link, "pubDate", guid, "feedID", "cancelTF", "delayTF", "changeTF", priority, line)
      VALUES ${dbInputString};`)
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

//Main function
(async() => {
  const overallT0 = process.hrtime();
  let RSSFeedURL = 'https://www.njtransit.com/rss/RailAdvisories_feed.xml';
  let feedPromise = incomingFeed(RSSFeedURL);
  let guidArrayPromise = last100Items();
  await Promise.all([feedPromise, guidArrayPromise]).then((values) => {
    let feed = values[0];
    let oldGuidArray = values[1]; 
    const writeArray = filterArray(feed, oldGuidArray);
    if (writeArray.length > 0) {
      writeItemsToDb(writeArray, overallT0);
    } else {
      console.log('writeItemsToDb wrote 0 rows (All items were duplicates or matched keywords).');
      console.log('RSS Sorter Lambda took ' + ((process.hrtime(overallT0)[1]) / 1e9) + ' seconds to run.')
    }
  })
})();

