import { connect, execSql, close, isDbAvailable } from "./db/connection.js";

async function main() {
  console.log("Testing DB connection...");

  const available = await isDbAvailable();
  console.log("Available:", available);

  if (!available) {
    console.log("DB not available, exiting");
    process.exit(1);
  }

  const conn = await connect();
  console.log("Connected!");

  const rows = await execSql(conn, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'");
  console.log("Tables:", rows.map(r => r[0].value));

  const count = await execSql(conn, "SELECT COUNT(*) AS cnt FROM tbImportLabsData");
  console.log("Labs rows:", count[0]?.[0]?.value);

  await close();
  console.log("Done");
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
