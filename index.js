import { MongoClient } from "mongodb";
import { clearInfluxDb, writeToInflux } from "./InFlux/index.js";
import { dataFields } from "./general/main.js";
import { testTimescaleConnection } from "./timescale/main.js";
import {
  initatePostgresql,
  testPostgresql,
  testReadingPostgresql,
  writeToPostgresql,
} from "./postgresql/main.js";

const chunk = 1000;

export const populateDBS = async () => {
  var totalElements = 0;
  var total = 0;
  var count = 0;
  var countWrite = 0;
  getDbClient(async (client) => {
    const db = client.db("farming");
    const locatii = db.collection("locatii");

    const query = {
      lat: { $gte: 45 },
      lng: { $gte: 25 },
      timestamp: {
        $gte: new Date("2023-09-01T00:00:00.000Z"),
        $lte: new Date("2023-09-14T23:00:00.000Z"),
      },
    };

    const cursor = locatii.find(query);

    console.info("Count total elements");
    totalElements = await locatii.countDocuments(query);

    const s = cursor.stream();

    s.on("data", (d) => {
      // write to influx
      count++;
      if (count == chunk) {
        total += count;
        console.info("Elements read ", total, " out of ", totalElements);
        count = 0;
        s.pause();
      } else if (total == totalElements) {
        console.info("All elementes readed");
        s.close();
      }

      writeToPostgresql(
        {
          timestamp: d.timestamp,
          data: d.data,
          id: d.imei,
        },
        (done) => {
          countWrite++;
          if (countWrite == chunk) {
            // stream is paused, resume it after 10 seconds
            countWrite = 0;
            s.resume();
          }
        }
      );

      //writeToInflux(d);
    });

    cursor.on("end", () => {
      console.info(
        `Cursor end after ${total} elements, End time ${new Date().toTimeString()}`
      );
      cursor.close();
    });
  });
};

export const getDbClient = async (cl) => {
  const dbUrl =
    "mongodb://readWrite:XFj038Rg4bEH@altimanager.ro:27021/farming?authMechanism=DEFAULT&authSource=admin";
  const client = new MongoClient(dbUrl);
  await client.connect();
  cl(client);
};

// start with arg
process.argv.forEach((a, index) => {
  if (index == 2) {
    let db = process.argv[3];
    switch (a) {
      case "populateDBs":
        populateDBS();
        break;
      case "initiate":
        switch (db) {
          case "influx":
            clearInfluxDb();
            break;
          case "postgresql":
            initatePostgresql();
            break;
          default:
            break;
        }
        break;
      case "testRead":
        switch (db) {
          case "postgresql":
            // test postgresql
            testReadingPostgresql();
            break;

          default:
            break;
        }
        break;
      case "testTimescale":
        testTimescaleConnection();
        break;
      case "testPostgresql":
        testPostgresql();
        break;
      default:
        console.info("Not valid argument", a);
        break;
    }
  }
});
