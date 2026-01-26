import {
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from "@aws-sdk/client-athena";
import { athenaClient } from "./athenaClient.js";

const WORKGROUP = process.env.ATHENA_WORKGROUP || "primary";
const S3_OUTPUT = process.env.ATHENA_OUTPUT || "s3://phillyscan-athena-results/athena-results/";
const DB = process.env.ATHENA_DATABASE || "philly_data";

async function start(query, allowResultCfg) {
  const params = {
    QueryString: query,
    QueryExecutionContext: { Database: DB },
    WorkGroup: WORKGROUP,
  };
  if (allowResultCfg) params.ResultConfiguration = { OutputLocation: S3_OUTPUT };
  return athenaClient.send(new StartQueryExecutionCommand(params));
}

export async function runAthena(query) {
  let qid;
  try {
    const s1 = await start(query, true);
    qid = s1.QueryExecutionId;
  } catch (e) {
    // retry if the workgroup rejects custom output
    if (String(e.message || "").match(/workgroup|ResultConfiguration|OutputLocation/i)) {
      const s2 = await start(query, false);
      qid = s2.QueryExecutionId;
    } else throw e;
  }

  while (true) {
    const q = await athenaClient.send(new GetQueryExecutionCommand({ QueryExecutionId: qid }));
    const state = q.QueryExecution.Status.State;
    if (state === "SUCCEEDED") break;
    if (state === "FAILED" || state === "CANCELLED")
      throw new Error(`Athena ${state}: ${q.QueryExecution.Status.StateChangeReason || "unknown"}`);
    await new Promise(r => setTimeout(r, 600));
  }

  const res = await athenaClient.send(new GetQueryResultsCommand({ QueryExecutionId: qid }));
  const rows = res.ResultSet.Rows.map(r => r.Data.map(d => d.VarCharValue ?? null));
  const [header, ...data] = rows;
  if (!header) return [];
  return data.map(row => Object.fromEntries(row.map((v,i)=>[header[i], v])));
}

