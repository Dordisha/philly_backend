cat <<'EOF' > src/aws/athenaClient.js
import { AthenaClient } from "@aws-sdk/client-athena";

const { AWS_REGION = "us-east-1" } = process.env;

export const athenaClient = new AthenaClient({
  region: AWS_REGION,
  // Credentials auto-resolve from env vars, AWS config, or IAM role.
});
EOF
