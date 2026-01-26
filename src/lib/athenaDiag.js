import { runAthena } from "./athenaRun.js";

export async function athenaSelect1() {
  return runAthena("SELECT 1");
}
export async function athenaListTables() {
  return runAthena("SHOW TABLES");
}
export async function athenaDescribe(table) {
  return runAthena(`DESCRIBE ${table}`);
}
export async function athenaSample(table, limit = 5) {
  return runAthena(`SELECT * FROM ${table} LIMIT ${limit}`);
}

