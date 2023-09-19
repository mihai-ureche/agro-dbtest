import pgk from "pg";
import { dataFields } from "../general/main.js";
const { Client, Pool } = pgk;
const URL = "postgresql://alti:lowvoice142@127.0.0.1:5432/loc";
const eliminateFields = ["latitude", "longitude", "altitude"];

var ConnectionPool = null;

const createPool = () => {
  const pool = new Pool({
    host: "127.0.0.1",
    port: 5432,
    user: "alti",
    password: "lowvoice142",
    database: "loc",
    idleTimeoutMillis: 0,
    connectionTimeoutMillis: 0,
    min: 5,
    max: 50,
  });
  return pool;
};

export const getPool = () => {
  if (!ConnectionPool) {
    ConnectionPool = createPool();
  }
  return ConnectionPool;
};

export const testPostgresql = () => {
  let q =
    "CREATE TABLE IF NOT EXISTS testtable (id VARCHAR (50) PRIMARY KEY, testTime VARCHAR(50), created_on TIMESTAMP)";
  getPool().query(q, [], (err, result, next) => {
    console.log(err, result);
  });
};

export const initatePostgresql = () => {
  let q = "";
  // clear DB

  // clear positions Table
  q = `DROP TABLE IF EXISTS telemetry;
  DROP TABLE IF EXISTS devices;
  DROP TABLE IF EXISTS positions;`;

  getPool().query(q, [], (err, result) => {
    if (err) {
      console.error(">>> E >>> ", err);
    }
    // generate empty tables

    let columns = dataFields
      .map((f) => {
        let type = "";
        switch (f.type) {
          case "float":
            type = "NUMERIC";
            break;

          default:
            type = "VARCHAR(500)";
            break;
        }

        if (!eliminateFields.includes(f.value)) {
          return `${f.value} ${type}`;
        } else {
          return null;
        }
      })
      .filter((f) => f)
      .join(",");
    q = `CREATE TABLE IF NOT EXISTS devices (id SERIAL PRIMARY KEY, name VARCHAR(100), imei VARCHAR(100) UNIQUE);
     CREATE TABLE IF NOT EXISTS positions (id SERIAL PRIMARY KEY, latitude NUMERIC, longitude NUMERIC, altitude NUMERIC, time timestamp ,imei VARCHAR(50) );
     CREATE TABLE IF NOT EXISTS telemetry(id SERIAL,${columns}, imei VARCHAR(100))`;

    getPool().query(q, [], (err, result) => {
      if (err) {
        console.error(">>> E >>> ", err);
      }
      console.info("Initialization DONE");
    });
  });
};

export const writeToPostgresql = (d, cb) => {
  // create writeing flow
  let q = `INSERT INTO positions(latitude, longitude, altitude,time, imei) VALUES(${
    d.data.latitude
  }, ${d.data.longitude}, ${d.data.altitude}, '${new Date(
    d.data.time
  ).toISOString()}', '${d.id.trim()}') `;

  getPool().query(q, [], (err, result) => {
    if (err) {
      cb(false);
      console.error(">>> E >>> ", err, " >>> Q >>>> ", q);
    } else {
      cb(true);
    }
  });
};

export const generateRandomMinutes = (refDate, gap) => {
  const minMinutes = 120;
  const maxMinutes = 240;

  const r = new Date(refDate);
  const addMinutes = Math.floor(
    Math.random() * (maxMinutes - minMinutes) + minMinutes
  );

  var startDate = new Date(r.getTime() + addMinutes * 60 * 1000);
  var endDate = new Date(startDate.getTime() + gap * 60 * 60 * 1000);

  return [startDate, endDate];
};

export const testReadingPostgresql = () => {
  const runTest = (opt, done) => {
    let interval = generateRandomMinutes(opt.refDate, opt.gap);

    let q = "";
    // read 1h hour for one device
    q = `SELECT * FROM positions 
    WHERE time >= '${interval[0].toISOString()}'
    AND time <= '${interval[1].toISOString()}'
    AND imei = '002002604333'
      ORDER BY time DESC`;

    let t0 = new Date();

    console.info(
      "Getting points for interval ",
      interval[0],
      " - ",
      interval[1],
      " for device 002002604333"
    );

    getPool().query(q, [], (err, result) => {
      let t1 = new Date();
      let gap = (t1.getTime() - t0.getTime()) / 1000;

      console.info(
        ">>>> ",
        opt.gap,
        " hours  interval. We have results after ",
        gap,
        " seconds. ",
        "Total points ",
        result.rowCount
      );

      // call done
      done(true);
    });
  };

  var c = 0;
  const startTest = () => {
    let refDate = "2023-09-7";
    let gap = 1;

    if (c >= 5 && c < 10) {
      refDate = "2023-09-01";
      gap = 48;
    } else if (c >= 10) {
      refDate = "2023-09-01";
      gap = 168;
    }

    runTest(
      {
        refDate,
        gap,
      },
      (done) => {
        c++;
        if (c < 15) {
          startTest();
        }
      }
    );
  };

  startTest();
};
