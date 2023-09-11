import { InfluxDBClient, Point } from "@influxdata/influxdb3-client";
import { dataFields } from "../general/main.js";

const database = `ALTI_TEST`;

async function main() {
  const client = new InfluxDBClient({
    host: "https://westeurope-1.azure.cloud2.influxdata.com",
    token: token,
  });

  // following code goes here

  let database = `ALTI_TEST`;

  const points = [
    new Point("census").tag("location", "Klamath").intField("bees", 23),
    new Point("census").tag("location", "Portland").intField("ants", 30),
    new Point("census").tag("location", "Klamath").intField("bees", 28),
    new Point("census").tag("location", "Portland").intField("ants", 32),
    new Point("census").tag("location", "Klamath").intField("bees", 29),
    new Point("census").tag("location", "Portland").intField("ants", 40),
  ];

  for (let i = 0; i < points.length; i++) {
    const point = points[i];
    await client
      .write(point, database)
      // separate points by 1 second
      .then(() => new Promise((resolve) => setTimeout(resolve, 1000)));
  }

  const query = `SELECT * FROM 'census' WHERE time >= now() - interval '24 hours' AND ('bees' IS NOT NULL OR 'ants' IS NOT NULL) order by time asc`;

  console.log(query);

  const rows = await client.query(query, "ALTI_TEST");
  console.log(rows);

  client.close();
}

export const getInfluxClient = (cl) => {
  const token =
    "n4dUnvHs1TklPxBeuDlS_ZXjJgBRuVSFITpIbJt0TfP_sIHl8ifogQ59FOGv11dXiIOB01UTjE4P3j5NKK6AIA==";
  const client = new InfluxDBClient({
    host: "https://westeurope-1.azure.cloud2.influxdata.com",
    token: token,
  });
  cl(client);
};

export const writeToInflux = (d) => {
  getInfluxClient((cl) => {
    let p = new Point("loc").tag("id", d.id).timestamp(d.timestamp);

    dataFields.forEach((f) => {
      let val = d.data[f.value];
      if (val) {
        switch (f.type) {
          case "string":
            p.stringField(f.value, d.data[f.value]);
            break;
          default:
            // default as float
            p.floatField(f.value, d.data[f.value]);
            break;
        }
      }
    });

    cl.write(p, database)
      .then((done) => {
        if (done) {
          console.info("Writed ", done);
        }
      })
      .catch((e) => {
        if (e) {
          console.error("We have error >>>  ", e);
        }
      });
  });
};

export const clearInfluxDb = () => {
  getInfluxClient(async (cl) => {
    let q = `SELECT * FROM "loc"`;
    let qType = "sql";

    try {
      const r = cl.query(q, database, qType);

      for await (const val of r) {
        console.log(val);
      }
    } catch (e) {
      console.error(e);
    }
  });
};
