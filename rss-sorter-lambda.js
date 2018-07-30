require('dotenv').config()

const { Pool } = require('pg')
let Parser = require('rss-parser');
let parser = new Parser();

// pools will use environment variables
// for connection information

const pool = new Pool();

const guidArray = [];

pool.query('SELECT guid FROM feedDetails ORDER BY pubDate LIMIT 10', [1])
  .then(
    res.rows.forEach(guid => {
      guidArray.push(guid);
    })
  )
  .catch(e => setImmediate(() => { throw e }))

(async () => {
 
  let feed = await parser.parseURL('https://www.njtransit.com/rss/RailAdvisories_feed.xml');
  console.log(feed.title);
 
  feed.items.forEach(item => {
    //console.log(item.title + ':' + item.link)
    if (guidArray.find(i => i === item.guid) === undefined) {
        const guid = item.guid;
        const title = item.title;
        const description = item.description;
        const link = item.link;
        const pubDate = item.pubDate;
    }
    pool.query(`INSERT INTO feedDetails (title, description, link, pubDate, guid, feedID) VALUES (${title}, ${description}, ${link}, ${pubDate}, ${guid}, 1)`, [1])
      .then(
          res => console.log('item:', res.rows[0])
        )
      .catch(e => setImmediate(() => { throw e }))
  });
 
})();

//
// import sys
// import logging
// import rds_config
// import pymysql
// #rds settings
// rds_host  = "rds-instance-endpoint"
// name = rds_config.db_username
// password = rds_config.db_password
// db_name = rds_config.db_name


// logger = logging.getLogger()
// logger.setLevel(logging.INFO)

// try:
//     conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
// except:
//     logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
//     sys.exit()

// logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
// def handler(event, context):
//     """
//     This function fetches content from mysql RDS instance
//     """

//     item_count = 0

//     with conn.cursor() as cur:
//         cur.execute("create table Employee3 ( EmpID  int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (EmpID))")  
//         cur.execute('insert into Employee3 (EmpID, Name) values(1, "Joe")')
//         cur.execute('insert into Employee3 (EmpID, Name) values(2, "Bob")')
//         cur.execute('insert into Employee3 (EmpID, Name) values(3, "Mary")')
//         conn.commit()
//         cur.execute("select * from Employee3")
//         for row in cur:
//             item_count += 1
//             logger.info(row)
//             #print(row)
//      conn.commit()

//     return "Added %d items from RDS MySQL table" %(item_count)