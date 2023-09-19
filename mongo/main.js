import { MongoClient } from "mongodb";
var cl = null;
var db = null;
var locatii = null;

// write to Mongod
export const createDbClient = async (c) => {
  const dbUrl = "mongodb://127.0.0.1:27021/farming";
  const client = new MongoClient(dbUrl);
  await client.connect();
  cl = client;
  db = client.db("farming");
  locatii = db.collection("locatii");
  c(client);
};

export const writeToMongod = (d, cb) => {
  const write = () => {
    const doc = {
      imei: d.id,
      timestamp: d.timestamp,
      latitude: d.data.latitude,
      longitude: d.data.longitude,
      altitude: d.data.altitude,
    };
    locatii
      .insertOne(doc)
      .then((r) => {
        cb(true);
      })
      .catch((e) => {
        console.error(e);
        cb(false);
      });
  };

  if (cl) {
    // we have CL
    write();
  } else {
    createDbClient((c) => {
      /// we can write;
      write();
    });
  }
};
