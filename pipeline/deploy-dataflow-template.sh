#!/usr/bin/env bash
set -e
set -u

export TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/dataflow/event-processing:latest"
export TEMPLATE_PATH="gs://${METADATA_BUCKET}/event-processing-pipeline.json"

echo "Deploying dataflow template to: ${TEMPLATE_IMAGE}"

./gradlew getDependencies

JAR_LIST="--jar build/libs/pipeline-0.1.0.jar"
for d in build/dependencies/* ;
do
  JAR_LIST="${JAR_LIST} --jar $d"
done

gcloud dataflow flex-template build ${TEMPLATE_PATH} \
  --image-gcr-path "${TEMPLATE_IMAGE}" \
  --sdk-language "JAVA" \
  --flex-template-base-image JAVA11 \
  --metadata-file "pipeline-template-metadata.json" \
  ${JAR_LIST} \
  --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.solutions.pipeline.EventProcessingPipeline"
