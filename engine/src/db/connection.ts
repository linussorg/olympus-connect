import { Connection, Request, TYPES, ColumnValue } from "tedious";

const DB_HOST = process.env.DB_HOST || "localhost";
const DB_PORT = parseInt(process.env.DB_PORT || "1433", 10);
const DB_USER = process.env.DB_USER || "sa";
const DB_PASSWORD = process.env.DB_PASSWORD || "Hack2026Pass";
const DB_NAME = process.env.DB_NAME || "Hack2026";

let _connection: Connection | null = null;

export function getConnectionConfig() {
  return {
    server: DB_HOST,
    options: {
      port: DB_PORT,
      database: DB_NAME,
      trustServerCertificate: true,
      encrypt: true,
      rowCollectionOnRequestCompletion: true,
    },
    authentication: {
      type: "default" as const,
      options: { userName: DB_USER, password: DB_PASSWORD },
    },
  };
}

export function connect(): Promise<Connection> {
  return new Promise((resolve, reject) => {
    if (_connection) {
      resolve(_connection);
      return;
    }

    const config = getConnectionConfig();
    const conn = new Connection(config);

    conn.on("connect", (err) => {
      if (err) {
        reject(new Error(`SQL Server connection failed: ${err.message}`));
      } else {
        _connection = conn;
        resolve(conn);
      }
    });

    conn.on("error", (err) => {
      console.error("SQL Server connection error:", err.message);
      _connection = null;
    });

    conn.connect();
  });
}

export function execSql(conn: Connection, query: string): Promise<ColumnValue[][]> {
  return new Promise((resolve, reject) => {
    const rows: ColumnValue[][] = [];
    const request = new Request(query, (err, rowCount, resultRows) => {
      if (err) reject(err);
      else resolve(resultRows || rows);
    });

    request.on("row", (columns) => {
      rows.push(columns);
    });

    conn.execSql(request);
  });
}

/** Create a fresh (non-singleton) connection for parallel-safe operations. Caller must close it. */
export function connectNew(): Promise<Connection> {
  return new Promise((resolve, reject) => {
    const config = getConnectionConfig();
    const conn = new Connection(config);
    conn.on("connect", (err) => {
      if (err) reject(new Error(`SQL Server connection failed: ${err.message}`));
      else resolve(conn);
    });
    conn.on("error", (err) => {
      console.error("SQL Server connection error:", err.message);
    });
    conn.connect();
  });
}

export function closeConnection(conn: Connection): Promise<void> {
  return new Promise((resolve) => {
    conn.on("end", () => resolve());
    conn.close();
  });
}

export function close(): Promise<void> {
  return new Promise((resolve) => {
    if (_connection) {
      _connection.on("end", () => {
        _connection = null;
        resolve();
      });
      _connection.close();
    } else {
      resolve();
    }
  });
}

export async function isDbAvailable(): Promise<boolean> {
  try {
    const conn = await connect();
    await execSql(conn, "SELECT 1 AS ok");
    return true;
  } catch (err) {
    console.error("DB availability check failed:", err instanceof Error ? err.message : err);
    // Clear stale cached connection so next connect() attempt starts fresh
    _connection = null;
    return false;
  }
}
