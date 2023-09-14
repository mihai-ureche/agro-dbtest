import Sequelize from "sequelize";

const conString = `postgres://tsdbadmin:vQ44u-Lwe-Y2A2@qb87j333av.wejm8xhxcf.tsdb.cloud.timescale.com:36196/tsdb`;

export const testTimescaleConnection = () => {
  const sequelize = new Sequelize(conString, {
    dialect: "postgres",
    protocol: "postgres",
    dialectOptions: {
      ssl: {
        require: true,
        rejectUnauthorized: false,
      },
    },
  });

  sequelize
    .authenticate()
    .then(() => {
      console.log("Connection has been established successfully.");
    })
    .catch((err) => {
      console.error("Unable to connect to the database:", err);
    });
};
