#!/usr/bin/env bash
set -u

TEMPLATE_PATH="gs://${METADATA_BUCKET}/event-processing-pipeline.json"

set +u
EXTRA_OPTIONS=""
if [[ ! -z "${DF_NETWORK}" ]]; then
  EXTRA_OPTIONS="${EXTRA_OPTIONS} --network=${DF_NETWORK}"
fi

if [[ ! -z "${DF_SUBNETWORK}" ]]; then
  EXTRA_OPTIONS="${EXTRA_OPTIONS} --subnetwork=${DF_SUBNETWORK}"
fi
set -u

set -x
gcloud dataflow flex-template run event-processing \
    --template-file-gcs-location "${TEMPLATE_PATH}" \
    --project=${PROJECT_ID} \
    --region ${REGION} \
    --max-workers=5 \
    --enable-streaming-engine \
    ${EXTRA_OPTIONS}  \
    --parameters subscriptionId=${EVENT_SUB} \
    --parameters suspiciousActivityTopic=${SUSPICIOUS_ACTIVITY_TOPIC} \
    --parameters datasetName=${DATASET}
set +x

#    --service-account-email ${DATAFLOW_WORKER_SA} \
#    --staging-location gs://${DF_TEMP_BUCKET}/tmp \

