import { AthenaClient } from "@aws-sdk/client-athena";

export const athenaClient = new AthenaClient({
  region: process.env.AWS_REGION || "us-east-1",
  // creds: default provider chain (env vars, shared config/SSO, etc.)
});
