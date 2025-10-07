#!/usr/bin/env bash
set -euo pipefail
if [[ -f ".env" ]]; then set -a; source ./.env; set +a; fi
: "${AWS_REGION:=us-east-1}"; : "${STAGE:=dev}"
if command -v sls >/dev/null 2>&1; then SLS=sls; elif command -v serverless >/dev/null 2>&1; then SLS=serverless; else echo "Instala serverless: npm i -g serverless"; exit 1; fi
$SLS deploy --region "$AWS_REGION" --stage "$STAGE"
$SLS info   --region "$AWS_REGION" --stage "$STAGE" || true
