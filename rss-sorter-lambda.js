'use strict';

require('dotenv').config()

const { Client } = require('pg');
let Parser = require('rss-parser');
let parser = new Parser();

// clients will use environment variables
// for connection information
const client = new Client();

async function incomingFeed(feedURL) {
  console.log('Parse incoming feed started.');
  try {
    let feedItems = await parser.parseURL(feedURL);
    console.log(feedItems);
    return feedItems
  }
  catch(e) {
    console.log(`Incoming feed error: ${e}`);
  }
  console.log('Parse incoming feed finished.');
}

async function last20Items() {
  console.log('Get last 20 guids started.');
  try {
    const guidArr = [];
    await client.connect();
    let last20 = await client.query('SELECT guid FROM "feedDetails" ORDER BY "pubDate" DESC LIMIT 20');
    last20.rows.forEach(guid => {
      guidArr.push(guid);
    })
    client.end();
    return last20;
  }
  catch(e) {
    console.log(`DB read error: ${e}`);
  }
  console.log('Get last 20 guids finished.');
}

async function writeItemsToDb(newFeed, oldGuids) {
  //TODO: Split this into two functions, one for filter, second for write.
  console.log('Feed dupe filter started.');
  let itemsArr = [];
  newFeed.items.forEach((item) => {
    if (oldGuids.rows.includes(item.guid) === false) {
      const itemToAdd = ` ('${item.title}', '${item.content}', '${item.link}', '${item.pubDate}', '${item.guid}')`;
      itemsArr.push(itemToAdd);
    }
  })
  console.log('Feed dupe filter finished.');
  console.log(itemsArr);
  let items = itemsArr.join();
  items = items + ';';
  console.log(items);
  console.log('Write feed to DB started.')
  try {
    const client2 = new Client();
    await client2.connect();
    await client2.query(`INSERT INTO "feedDetails" ("title", "description", "link", "pubDate", "guid", "feedID")
    VALUES ${items};`);
    await client2.end();
  }
  catch(e) {
    console.log(`DB write error: ${e}`);
  }
  console.log('Write feed to DB finished.')
}

(async() => {
  let RSSFeedURL = 'https://www.njtransit.com/rss/RailAdvisories_feed.xml';
  let feedPromise = incomingFeed(RSSFeedURL);
  let guidPromise = last20Items();
  let feed = await feedPromise;
  let guidArray = await guidPromise;
  writeItemsToDb(feed, guidArray);
})();

