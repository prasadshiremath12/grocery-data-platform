#!/usr/bin/env bash
# One-time GCP setup. Usage: PROJECT=my-proj REGION=asia-south1 BUCKET=my-grocery-bucket ./scripts/setup_gcp.sh
set -euo pipefail
: "${PROJECT:?}" "${BUCKET:?}"
REGION="${REGION:-asia-south1}"

gcloud config set project "$PROJECT"
gcloud services enable dataflow.googleapis.com bigquery.googleapis.com storage.googleapis.com composer.googleapis.com

gsutil mb -l "$REGION" "gs://$BUCKET" || true
bq --location="$REGION" mk -d --description "Grocery analytics" grocery || true

# Service account for Dataflow workers
gcloud iam service-accounts create grocery-dataflow --display-name "Grocery Dataflow" || true
SA="grocery-dataflow@$PROJECT.iam.gserviceaccount.com"
for role in roles/dataflow.worker roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" --role="$role" --quiet >/dev/null
done
echo "Done. Upload CSVs:  gsutil cp data/*.csv gs://$BUCKET/landing/\$(date +%F)/"
