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

import com.google.solutions.pipeline.model.ProcessInfo;
import com.google.solutions.pipeline.service.ProcessInfoRetriever;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;

public class ProcessMetadataSupplier extends DoFn<Long, Map<String, ProcessInfo>> {
  private static final long serialVersionUID = 1L;

  private final String datasetName;
  private final String processInfoTableName;

  public ProcessMetadataSupplier(String datasetName, String processInfoTableName) {
    this.datasetName = datasetName;
    this.processInfoTableName = processInfoTableName;
  }

  @ProcessElement
    public void process(OutputReceiver<Map<String, ProcessInfo>> o)
                          throws InterruptedException {
      o.output(new ProcessInfoRetriever().getProcessInfo(datasetName, processInfoTableName));
    }
}
