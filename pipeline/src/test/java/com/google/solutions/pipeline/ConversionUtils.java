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

package com.google.solutions.pipeline;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.solutions.pipeline.converter.ProxyRequestConverter;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.ProcessInfo;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Instant;

public class ConversionUtils {

  public static Event fromString(String payload, Map<String, ProcessInfo> processInfoMap) {
    try {
      GenericJson json = new JacksonFactory().createJsonParser(payload)
          .parse(GenericJson.class);
      return new ProxyRequestConverter(processInfoMap).
          convert(json);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse payload: ", e);
    }
  }

  public static PubsubMessage validPubsubMessage(Instant time, String sourceIp, String destinationIp,
      int destinationPort, int bytesSent, int bytesReceived, String processName, String user) {
    // TODO: pretty bad; convert to use a JSON framework.
    String payload = "{\"bytes_sent\": " + bytesSent + ", "
        + "\"destination_ip\": \"" + destinationIp + "\", "
        + "\"destination_port\": " + destinationPort + ", "
        + "\"process\": \"" + processName + "\", "
        + " \"request_timestamp\": " + time.getMillis() + ", "
        + "\"bytes_received\": " + bytesReceived + ", "
        + "\"source_ip\": \"" + sourceIp + "\""
        + (user == null ? "" : ", \"user\": \"" + user + "\"")
        + "}";

    return pubsubMessage(payload);
  }

  public static PubsubMessage pubsubMessage(String payload) {
    return new PubsubMessage(payload.getBytes(), null, UUID.randomUUID().toString());
  }
}
