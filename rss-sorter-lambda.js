'use strict';

require('dotenv').config()

const { Pool } = require('pg')
let Parser = require('rss-parser');
let parser = new Parser();

// pools will use environment variables
// for connection information
const pool = new Pool();

async function incomingFeed(feedURL) {
  try {
    let feedItems = await parser.parseURL(feedURL);
    console.log(feedItems);
    return feedItems
  }
  catch(e) {
    console.log(`Incoming feed error: ${e}`);
  }
}

async function last20Items() {
  try {
    const guidArr = [];
    let last20 = await pool.query('SELECT guid FROM "feedDetails" ORDER BY "pubDate" DESC LIMIT 20');
    last20.rows.forEach(guid => {
      guidArr.push(guid);
    })
    return last20;
  }
  catch(e) {
    console.log(`DB read error: ${e}`);
  }
}

function writeItemsToDb(newFeed, oldGuids) {
  newFeed.items.forEach((item) => {
    //console.log(item.title + ':' + item.link)
    if (oldGuids.includes(item.guid) === false) {
      try {
        pool.query(`INSERT INTO "feedDetails" ("title", "description", "link", "pubDate", "guid", "feedID") VALUES (${item.title}, ${item.description}, ${item.link}, ${item.pubDate}, ${item.guid}, 1)`, [1])
      }
      catch(e) {
        console.log(`DB write error: ${e}`);
      }
    }
  })
}

(async() => {
  let RSSFeedURL = 'https://www.njtransit.com/rss/RailAdvisories_feed.xml';
  let feedPromise = incomingFeed(RSSFeedURL);
  let guidPromise = last20Items();
  let feed = await feedPromise;
  let guidArray = await guidPromise;
  writeItemsToDb(feed, guidArray);
})();

