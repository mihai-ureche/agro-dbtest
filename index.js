import { MongoClient } from "mongodb";
import {
  clearWriteApi,
  getWriteApi,
  testReadingInflux,
  writeToInflux,
} from "./InFlux/main.js";
import { dataFields } from "./general/main.js";
import { testTimescaleConnection } from "./timescale/main.js";
import {
  initatePostgresql,
  testPostgresql,
  testReadingPostgresql,
  writeToPostgresql,
} from "./postgresql/main.js";
import sizeof from "object-sizeof";
import { writeToMongod } from "./mongo/main.js";

const chunk = 200;
var totalElements = 0;
var total = 0;
var count = 0;
var countWrite = 0;

export const populateMongoDB = () => {
  // populate mongodb

  getCursor((cursor) => {
    const s = cursor.stream();

    s.on("data", (d) => {
      count++;
      if (count == chunk) {
        total += count;
        console.info("Elements read ", total, " out of ", totalElements);
        count = 0;
        s.pause();
      }
      writeToMongod(d, (cb) => {
        countWrite++;
        if (countWrite == chunk) {
          // stream is paused, resume it after 10 seconds
          countWrite = 0;
          setTimeout(() => {
            s.resume();
          });
        }
      });
    });

    s.on("end", () => {
      console.info("Done writeig points");
      s.close();
    });
  });
};

export const populateInflux = () => {
  // populate influx DB
  getCursor((cursor) => {
    const s = cursor.stream();

    s.on("data", (d) => {
      // write to influx
      count++;
      if (count == chunk) {
        total += count;
        console.info("Elements read ", total, " out of ", totalElements);
        count = 0;
        s.pause();

        // suspend writeing
        const w = getWriteApi();
        w.close();
        clearWriteApi();
      } else if (total == totalElements) {
        console.info("All elementes readed");
        s.close();
      }

      writeToInflux(
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
            setTimeout(() => {
              s.resume();
            }, 1000);
          }
        }
      );
    });

    cursor.on("end", () => {
      console.info(
        `Cursor end after ${total} elements, End time ${new Date().toTimeString()}`
      );
      cursor.close();
    });
  });
};

const getCursor = (c) => {
  getDbClient(async (client) => {
    const db = client.db("farming");
    const locatii = db.collection("locatii");

    const query = {
      lat: { $gte: 45 },
      lng: { $gte: 25 },
      timestamp: {
        $gte: new Date("2023-09-08T00:00:00.000Z"),
        $lte: new Date("2023-09-08T23:59:59.000Z"),
      },
    };

    const cursor = locatii.find(query);

    console.info("Count total elements");
    totalElements = await locatii.countDocuments(query);

    c(cursor);
  });
};

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
        $gte: new Date("2023-09-10T00:00:00.000Z"),
        $lte: new Date("2023-09-10T23:59:59.000Z"),
      },
      getDbClient,
    };

    const cursor = locatii.find(query);

    const s = cursor.stream();

    s.on("data", (d) => {
      // write to influx
      count++;
      if (count == chunk) {
        total += count;
        console.info("Elements read ", total, " out of ", totalElements);
        count = 0;
        s.pause();

        // suspend writeing
        const w = getWriteApi();
        w.close();
        clearWriteApi();
      } else if (total == totalElements) {
        console.info("All elementes readed");
        s.close();
      }

      writeToInflux(
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
            setTimeout(() => {
              s.resume();
            }, 1000);
          }
        }
      );

      // writeToPostgresql(
      //   {
      //     timestamp: d.timestamp,
      //     data: d.data,
      //     id: d.imei,
      //   },
      //   (done) => {
      //     countWrite++;
      //     if (countWrite == chunk) {
      //       // stream is paused, resume it after 10 seconds
      //       countWrite = 0;
      //       s.resume();
      //     }
      //   }
      // );

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
        switch (db) {
          case "influx":
            populateInflux();
            break;
          case "mongo":
            populateMongoDB();
            break;

          default:
            console.info("No info");
            break;
        }
        break;
      case "initiate":
        switch (db) {
          case "influx":
            console.info("No script");
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

          case "influx":
            // test influx
            testReadingInflux();

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
