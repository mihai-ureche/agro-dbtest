import { MongoClient } from "mongodb";
import { clearInfluxDb, writeToInflux } from "./InFlux/index.js";
import { dataFields } from "./general/main.js";

export const populateDBS = () => {
  var count = 0;
  getDbClient((client) => {
    const db = client.db("farming");
    const locatii = db.collection("locatii");

    const cursor = locatii.find({
      lat: { $gte: 45 },
      lng: { $gte: 25 },
      timestamp: {
        $gte: new Date("2023-09-10T00:00:00.000Z"),
        $lte: new Date("2023-09-10T00:10:00.000Z"),
      },
    });

    const s = cursor.stream();

    s.on("data", (d) => {
      // write to influx

      let o = {
        timestamp: d.timestamp,
        data: dataFields.map((f) => {
          return {
            [f.value]: d.data[f.value],
          };
        }),

        id: d.imei,
      };
      count++;
      if (count % 100 == 0) {
        console.info("Elements read ", count);
      }
      writeToInflux(d);
    });
    s.on("end", () => {
      console.info(
        `Cursor end after ${count} elements, End time ${new Date().toTimeString()}`
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
    switch (a) {
      case "populateDBs":
        populateDBS();
        break;
      case "clearDB":
        let db = process.argv[3];
        switch (db) {
          case "influx":
            clearInfluxDb();
            break;

          default:
            break;
        }
        break;
      default:
        console.info("Not valid argument", a);
        break;
    }
  }
});
