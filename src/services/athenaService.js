// src/services/athenaService.js
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from "@aws-sdk/client-athena";

const {
  AWS_REGION = "us-east-1",
  ATHENA_DATABASE,
  ATHENA_WORKGROUP = "primary",
  ATHENA_OUTPUT_S3,
} = process.env;

// Single Athena client
export const athenaClient = new AthenaClient({
  region: AWS_REGION,
});

/**
 * Run an Athena query and return { columns, rows }
 * NOTE: We do NOT try to verify or create the S3 output bucket here.
 */
export class AthenaService {
  static async query(sql) {
    if (!ATHENA_DATABASE) throw new Error("Missing ATHENA_DATABASE");
    if (!ATHENA_OUTPUT_S3) throw new Error("Missing ATHENA_OUTPUT_S3");

    // Start
    const start = await athenaClient.send(
      new StartQueryExecutionCommand({
        QueryString: sql,
        // If your account doesn't have "primary", change via .env ATHENA_WORKGROUP
        WorkGroup: ATHENA_WORKGROUP,
        QueryExecutionContext: { Database: ATHENA_DATABASE },
        ResultConfiguration: { OutputLocation: ATHENA_OUTPUT_S3 },
      })
    );

    const qid = start.QueryExecutionId;
    if (!qid) throw new Error("Failed to start Athena query");

    // Poll status
    const t0 = Date.now();
    const timeoutMs = 60_000; // 60s
    const backoffMs = 600; // poll interval

    while (true) {
      const exec = await athenaClient.send(
        new GetQueryExecutionCommand({ QueryExecutionId: qid })
      );

      const status = exec?.QueryExecution?.Status?.State;
      if (status === "SUCCEEDED") break;

      if (status === "FAILED" || status === "CANCELLED") {
        const reason =
          exec?.QueryExecution?.Status?.StateChangeReason || "Unknown";
        throw new Error(`Athena query ${status}: ${reason}`);
      }

      if (Date.now() - t0 > timeoutMs) {
        throw new Error("Athena query timed out");
      }

      await new Promise((r) => setTimeout(r, backoffMs));
    }

    // Read results (paginate)
    let nextToken;
    let columnInfo = null;
    const rows = [];

    do {
      const res = await athenaClient.send(
        new GetQueryResultsCommand({
          QueryExecutionId: qid,
          NextToken: nextToken,
        })
      );

      if (!columnInfo) columnInfo = res?.ResultSet?.ResultSetMetadata?.ColumnInfo;

      const resultRows = res?.ResultSet?.Rows || [];
      // Skip header row (first page includes column names as the first row)
      const startIdx = nextToken ? 0 : 1;

      for (let i = startIdx; i < resultRows.length; i++) {
        const data = resultRows[i]?.Data || [];
        const obj = {};
        columnInfo.forEach((col, idx) => {
          obj[col.Name] = data[idx]?.VarCharValue ?? null;
        });
        rows.push(obj);
      }

      nextToken = res.NextToken;
    } while (nextToken);

    return { columns: (columnInfo || []).map((c) => c.Name), rows };
  }
}
