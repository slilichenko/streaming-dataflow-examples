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
import com.google.solutions.pipeline.model.UserEventFinding;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;

public class UserEventFindingToJSonString extends DoFn<UserEventFinding, String> {

  private final static long serialVersionUID = 1L;

  private JacksonFactory jacksonFactory;

  @Setup
  public void setup() {
    jacksonFactory = new JacksonFactory();
  }

  @ProcessElement
  public void process(@Element UserEventFinding userEventFinding, OutputReceiver<String> out) {
    GenericJson result = new GenericJson();
    result.setFactory(jacksonFactory);

    result
        .set("type", "user-event-finding")
        .set("request_ts", userEventFinding.getRequestTime().toString())
        .set("user_id", userEventFinding.getUserId())
        .set("level", userEventFinding.getFinding().getLevel().toString())
        .set("description", userEventFinding.getFinding().getDescription());

    out.output(result.toString());
  }
}
