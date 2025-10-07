#!/usr/bin/env bash
set -euo pipefail
: "${BUCKET:?Debes exportar BUCKET}"
: "${REGION:=us-east-1}"

echo "Bucket: $BUCKET  |  Region: $REGION"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Creando bucket..."
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET"
  else
    aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$REGION"
  fi
fi

echo "Habilitando website hosting..."
aws s3 website "s3://$BUCKET" --index-document index.html --error-document index.html

echo "Subiendo docs/... "
aws s3 sync "$(dirname "$0")" "s3://$BUCKET" --delete --acl public-read

echo "Aplicando política pública de lectura (objetos)..."
cat > /tmp/policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[{
    "Sid":"PublicReadGetObject",
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::$BUCKET/*"]
  }]
}
EOF
aws s3api put-bucket-policy --bucket "$BUCKET" --policy file:///tmp/policy.json

echo "Listo. Abre:"
echo "  http://$BUCKET.s3-website-$REGION.amazonaws.com/index.html"
