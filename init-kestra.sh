#!/bin/sh
KESTRA_URL="http://kestra:8080"
KESTRA_AUTH="${KESTRA_ADMIN_USER}:${KESTRA_ADMIN_PASSWORD}"
NAMESPACE="prod"

echo "=== Deploying flows (create or update) ==="
for f in /flows/*.yml; do
  id=$(grep "^id:" "$f" | awk '{print $2}')
  ns=$(grep "^namespace:" "$f" | awk '{print $2}')
  code=$(curl -s -o /dev/null -w "%{http_code}" -u "$KESTRA_AUTH" \
    -X POST "$KESTRA_URL/api/v1/main/flows" \
    -H "Content-Type: application/x-yaml" \
    --data-binary "@$f")
  if [ "$code" = "422" ]; then
    curl -s -o /dev/null -w "%{http_code} updated $f\n" -u "$KESTRA_AUTH" \
      -X PUT "$KESTRA_URL/api/v1/main/flows/$ns/$id" \
      -H "Content-Type: application/x-yaml" \
      --data-binary "@$f"
  else
    echo "$code created $f"
  fi
done

echo "=== Uploading namespace files (scripts) ==="
/app/kestra namespace files update "$NAMESPACE" /scripts \
  --server="$KESTRA_URL" \
  --user="$KESTRA_AUTH"

echo "=== Done ==="
