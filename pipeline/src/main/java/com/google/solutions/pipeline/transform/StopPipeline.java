/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.pipeline.transform;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Update;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import com.google.solutions.pipeline.transform.StopPipeline.StopInstruction;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopPipeline extends PTransform<PCollection<StopInstruction>, PDone> {

  private static final long serialVersionUID = 1L;

  public enum HowToStop {
    cancel {
      String getRequestedState() {
        return "JOB_STATE_CANCELLED";
      }
    },
    drain {
      String getRequestedState() {
        return "JOB_STATE_DRAINED";
      }
    };

    abstract String getRequestedState();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class StopInstruction {

    abstract HowToStop getHow();

    abstract String getReason();

    public static StopInstruction create(HowToStop newHow, String newReason) {
      return builder()
          .setHow(newHow)
          .setReason(newReason)
          .build();
    }

    public static Builder builder() {
      return new AutoValue_StopPipeline_StopInstruction.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setHow(HowToStop newHow);

      public abstract Builder setReason(String newReason);

      public abstract StopInstruction build();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StopPipeline.class);

  @Override
  public PDone expand(PCollection<StopInstruction> input) {
    input.apply("Stop the pipeline", ParDo.of(new PipelineStopper()));
    return PDone.in(input.getPipeline());
  }

  static class PipelineStopper extends DoFn<StopInstruction, Boolean> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void stop(@Element StopInstruction instruction, ProcessContext context) {
      DataflowWorkerHarnessOptions harnessOptions = getDataflowJobId(context.getPipelineOptions());
      if (harnessOptions == null) {
        return;
      }
      String jobId = harnessOptions.getJobId();
      LOG.info("Attempting to " + instruction.getHow() +
          " the Dataflow job " + jobId + " because " + instruction.getReason());

      try {
        GoogleCredentials workerCredentials = GoogleCredentials.getApplicationDefault();
        Dataflow dataflow = new Dataflow.Builder(GoogleNetHttpTransport.newTrustedTransport(),
            new GsonFactory(), null)
            .setApplicationName("Dataflow Worker")
            .build();
        Job job = new Job().setRequestedState(instruction.getHow().getRequestedState());
        dataflow.projects().locations().jobs().update(
            harnessOptions.getProject(),
            harnessOptions.getRegion(),
            jobId, job).setAccessToken(workerCredentials.getAccessToken().getTokenValue()).execute();
        LOG.info("Successfully requested stopping the Dataflow job " + jobId);
      } catch (Exception e) {
        LOG.error("Failed to stop the Dataflow job " + jobId, e);
      }
    }
  }

  private static DataflowWorkerHarnessOptions getDataflowJobId(PipelineOptions options) {
    String jobId = null;
    try {
      DataflowWorkerHarnessOptions workerHarnessOptions = options
          .as(DataflowWorkerHarnessOptions.class);
      return workerHarnessOptions;
    } catch (Exception e) {
      LOG.error(
          "Unable to find Dataflow job id. You can ignore this message is you are running under a different runner.",
          e);
      return null;
    }
  }
}
