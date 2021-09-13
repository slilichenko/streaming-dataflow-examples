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

import com.google.auto.value.AutoValue;
import com.google.solutions.pipeline.transform.StopPipeline.StopInstruction;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopPipeline extends PTransform<PCollection<StopInstruction>, PDone> {

  public enum HowToStop {cancel, drain}

  @AutoValue
  public abstract static class StopInstruction {

    abstract HowToStop how();

    abstract String reason();

    public static StopInstruction create(HowToStop how, String reason) {
      return builder()
          .how(how)
          .reason(reason)
          .build();
    }

    public static Builder builder() {
      return new AutoValue_StopPipeline_StopInstruction.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder how(HowToStop how);

      public abstract Builder reason(String reason);

      public abstract StopInstruction build();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StopPipeline.class);

  @Override
  public PDone expand(PCollection<StopInstruction> input) {
    String jobId = null;
    try {
      DataflowWorkerHarnessOptions workerHarnessOptions =
          input.getPipeline().getOptions().as(DataflowWorkerHarnessOptions.class);
      jobId = workerHarnessOptions.getJobId();
    } catch (Exception e) {
      LOG.error(
          "Unable to find Dataflow job id. You can ignore this message is you are running under a different runner.",
          e);
    }
    if (jobId != null) {
      input.apply("Stop the pipeline", ParDo.of(new PipelineStopper(jobId)));
    }
    return PDone.in(input.getPipeline());
  }

  static class PipelineStopper extends DoFn<StopInstruction, Boolean> {

    private static final long serialVersionUID = 1L;
    private final String jobId;

    public PipelineStopper(String jobId) {
      this.jobId = jobId;
    }

    @ProcessElement
    public void stop(@Element String reason) {
      LOG.info("Attempting to stop the Dataflow job " + jobId);
    }
  }
}
