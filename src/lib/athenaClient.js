import { AthenaClient } from "@aws-sdk/client-athena";

const region =
  process.env.AWS_REGION ||
  process.env.AWS_DEFAULT_REGION ||
  "us-east-1";

const accessKeyId = process.env.AWS_ACCESS_KEY_ID || "";
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || "";
const sessionToken = process.env.AWS_SESSION_TOKEN || undefined;

const cfg = { region };

// Force env credentials when present (removes “provider chain” ambiguity on Render)
if (accessKeyId && secretAccessKey) {
  cfg.credentials = {
    accessKeyId,
    secretAccessKey,
    ...(sessionToken ? { sessionToken } : {}),
  };
}

// Safe debug log (prints no secrets)
console.log(
  `[ATHENA] region=${region} envCreds=${Boolean(accessKeyId && secretAccessKey)}`
);

export const athenaClient = new AthenaClient(cfg);
