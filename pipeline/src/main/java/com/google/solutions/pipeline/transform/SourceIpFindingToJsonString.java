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

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.solutions.pipeline.model.SourceIpFinding;
import org.apache.beam.sdk.transforms.DoFn;

public class SourceIpFindingToJsonString extends DoFn<SourceIpFinding, String> {
  private final static long serialVersionUID = 1L;

  private JacksonFactory jacksonFactory;

  @Setup
  public void setup() {
    jacksonFactory = new JacksonFactory();
  }

  @ProcessElement
  public void process(@Element SourceIpFinding sourceIpFinding, OutputReceiver<String> out) {
    GenericJson result = new GenericJson();
    result.setFactory(jacksonFactory);

    result
        .set("type", "source-ip-finding")
        .set("request_ts", sourceIpFinding.getRequestTime().toString())
        .set("source_ip", sourceIpFinding.getSourceIp())
        .set("level", sourceIpFinding.getFinding().getLevel().toString())
        .set("description", sourceIpFinding.getFinding().getDescription());

    out.output(result.toString());
  }
}
