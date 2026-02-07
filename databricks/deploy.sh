#!/bin/bash
# Deploy Maryland Atlas pipeline to Azure Databricks.
#
# Prerequisites:
#   - Databricks CLI configured: databricks configure
#   - Python wheel build tools: pip install build
#
# Usage:
#   bash databricks/deploy.sh

set -e

echo "=== Maryland Atlas Databricks Deployment ==="

# Build wheel
echo ""
echo "1. Building Python wheel..."
python -m build --wheel --outdir dist/
WHEEL=$(ls -t dist/*.whl | head -1)
echo "   Built: $WHEEL"

# Upload wheel to DBFS
echo ""
echo "2. Uploading wheel to DBFS..."
databricks fs cp "$WHEEL" dbfs:/wheels/maryland_atlas-latest-py3-none-any.whl --overwrite
echo "   Uploaded to dbfs:/wheels/maryland_atlas-latest-py3-none-any.whl"

# Create or update workflow
echo ""
echo "3. Creating/updating Databricks workflow..."
databricks jobs create --json-file databricks/workflow.json 2>/dev/null || \
    echo "   Workflow may already exist. Update manually via Databricks UI or use 'databricks jobs reset'."

echo ""
echo "=== Deployment complete ==="
echo ""
echo "Next steps:"
echo "  1. Set env vars in your Databricks cluster/job:"
echo "     DATA_BACKEND=databricks"
echo "     DATABRICKS_SERVER_HOSTNAME=..."
echo "     DATABRICKS_HTTP_PATH=..."
echo "     DATABRICKS_ACCESS_TOKEN=..."
echo "     CENSUS_API_KEY=..."
echo "     AZURE_STORAGE_CONNECTION_STRING=..."
echo "  2. Initialize schema: python scripts/init_databricks.py --load-geometries"
echo "  3. Trigger the workflow: databricks jobs run-now --job-id <JOB_ID>"
