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
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.solutions.pipeline.EventProcessingPipeline;
import com.google.solutions.pipeline.converter.ProxyRequestConverter;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.InvalidPubSubMessage;
import com.google.solutions.pipeline.model.ProcessInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class PubSubMessageToEvent extends DoFn<PubsubMessage, Event> {
  private final static long serialVersionUID = 1L;

  private JacksonFactory jacksonFactory;
  private PCollectionView<Map<String,ProcessInfo>> processMetadataSideInput;

  public PubSubMessageToEvent(
      PCollectionView<Map<String, ProcessInfo>> processMetadataSideInput) {

    this.processMetadataSideInput = processMetadataSideInput;
  }

  @Setup
  public void setup() {
    jacksonFactory = new JacksonFactory();
  }

  @ProcessElement
  public void process(@Element PubsubMessage message, ProcessContext c) {

    GenericJson jsonPayload;
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(message.getPayload())) {
      JsonParser parser = jacksonFactory.createJsonParser(inputStream);
      jsonPayload = parser.parse(GenericJson.class);
    } catch (IOException e) {
      c.output(EventProcessingPipeline.invalidPubSubMessages,
          InvalidPubSubMessage
              .create("Failed to parse: " + e.getMessage(), message, c.timestamp()));
      EventProcessingPipeline.rejectedEvents.inc();
      return;
    }

    Map<String, ProcessInfo> processMetadata = c.sideInput(processMetadataSideInput);
    Event event;
    try {
      String payloadType = message.getAttributeMap() == null ? null : message.getAttribute("payloadType");
      payloadType = payloadType == null ? "Other" : payloadType;
      switch (payloadType) {
        case "type A":
          throw new IllegalStateException("type A parser not yet implemented");

        default:
          event = new ProxyRequestConverter(processMetadata).convert(jsonPayload);
      }
    } catch (Exception e) {
      c.output(EventProcessingPipeline.invalidPubSubMessages,
          InvalidPubSubMessage
              .create("Invalid payload: " + e.getMessage(), message, c.timestamp()));
      EventProcessingPipeline.rejectedEvents.inc();
      return;
    }

    c.output(event);
    EventProcessingPipeline.totalEvents.inc();
  }
}
