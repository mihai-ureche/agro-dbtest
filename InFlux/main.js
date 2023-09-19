import { InfluxDB, Point } from "@influxdata/influxdb-client";
import { generateRandomMinutes } from "../postgresql/main.js";
import sizeof from "object-sizeof";

const database = `ALTILOC`;
const org = "Altimanager";
const bucket = "ALTILOC";
const token =
  "n4dUnvHs1TklPxBeuDlS_ZXjJgBRuVSFITpIbJt0TfP_sIHl8ifogQ59FOGv11dXiIOB01UTjE4P3j5NKK6AIA==";
const url = "https://westeurope-1.azure.cloud2.influxdata.com";

var writeApi = null;

export const getInfluxClient = (cl) => {
  const token =
    "n4dUnvHs1TklPxBeuDlS_ZXjJgBRuVSFITpIbJt0TfP_sIHl8ifogQ59FOGv11dXiIOB01UTjE4P3j5NKK6AIA==";
  const client = new InfluxDBClient({
    host: "https://westeurope-1.azure.cloud2.influxdata.com",
    token: token,
  });
  cl(client);
};

const createWriteAPI = () => {
  writeApi = new InfluxDB({ url, token }).getWriteApi(org, bucket, "ns");
};

export const getWriteApi = () => {
  return writeApi;
};

export const clearWriteApi = () => {
  writeApi = null;
};

export const writeToInflux = async (d, cb) => {
  if (!writeApi) {
    createWriteAPI();
  }

  let p = new Point("loc").tag("id", d.id).timestamp(d.timestamp);
  p.floatField("latitude", d.data.latitude);
  p.floatField("longitude", d.data.longitude);
  p.floatField("altitude", d.data.altitude);
  p.stringField("imei", d.id);
  try {
    writeApi.writePoint(p);
  } catch (e) {
    console.error(e);
  }
  cb(true);
};

export const testReadingInflux = async () => {
  const queryApi = new InfluxDB({ url, token }).getQueryApi(org);

  const get = (cb) => {
    async function collectRows() {
      let t0 = new Date();
      const interval = generateRandomMinutes("2023-09-08", 4);

      const fluxQuery = `from(bucket: "ALTILOC") |>range(start: time(v: "${interval[0].toISOString()}"), stop: time(v: "${interval[1].toISOString()}"))  |> filter(fn: (r) => r._measurement == "loc")
      |> filter(
          fn: (r) => r._field == "altitude" or (r._field == "latitude" or r._field == "longitude"),
      )
      |> filter(fn: (r) => r.id == "002002604333")`;

      const data = await queryApi.collectRows(
        fluxQuery //, you can also specify a row mapper as a second argument
      );

      let size = sizeof(data) / 1024 / 1024;
      let t1 = new Date();
      let gap = (t1.getTime() - t0.getTime()) / 1000;

      console.info(
        ">>>> From ",
        interval[0],
        " to ",
        interval[1],
        " interval. We have results after ",
        gap,
        " seconds. ",
        "Total points ",
        data.length,
        " total size ",
        size,
        " MB"
      );

      cb(true);

      //data.forEach((x) => console.log(JSON.stringify(x.altitude)));
    }
    collectRows().catch((error) => console.error("CollectRows ERROR", error));
  };

  console.info("\n Run 5 test syncron");

  var c = 0;
  const startTest = () => {
    get((done) => {
      c++;
      if (c < 5) {
        startTest();
      } else {
        console.info("\n Run 5 test asyncron");
        // run 5 request concurentrly
        [0, 1, 2, 3, 4].forEach(() => {
          get((done) => {});
        });
      }
    });
  };

  startTest();
};
