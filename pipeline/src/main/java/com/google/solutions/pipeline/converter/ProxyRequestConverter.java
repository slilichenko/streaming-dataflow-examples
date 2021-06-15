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

package com.google.solutions.pipeline.converter;

import com.google.api.client.json.GenericJson;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.Event.Builder;
import com.google.solutions.pipeline.model.ProcessInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.joda.time.Instant;

public class ProxyRequestConverter implements EventConverter {

  private final Map<String, ProcessInfo> processMetadata;

  public ProxyRequestConverter(Map<String, ProcessInfo> processMetadata) {
    this.processMetadata = processMetadata;
  }

  @Override
  public Event convert(GenericJson json) throws IOException {
    Builder builder = Event.builder();

    String processName = nonRequired(json, "process");
    // TODO: add the enrichment here.
    String category = lookupCategoryInMetadata(processName);

    builder
        .setDestinationIp(required(json, "destination_ip"))
        .setSourceIp(required(json, "source_ip"))
        .setBytesSent(nonRequiredInt(json, "bytes_sent"))
        .setBytesReceived(nonRequiredInt(json, "bytes_received"))
        .setDestinationPort(nonRequiredInt(json, "destination_port"))
        .setProcessName(processName)
        .setUserId(nonRequired(json, "user"));

    long requestEpoch = ((BigDecimal) json.get("request_timestamp")).longValue();
    builder.setRequestTime(Instant.ofEpochMilli(requestEpoch));
    return builder.build();
  }

  private String lookupCategoryInMetadata(String processName) {
    String unknown = "UNKNOWN";
    if (processName == null) {
      return unknown;
    }
    ProcessInfo processInfo = processMetadata.get(processName);
    if (processInfo == null) {
      return unknown;
    }
    return processInfo.getCategory();
  }

  @SuppressWarnings("unchecked")
  private static <T> T required(GenericJson json, String fieldName) throws IOException {
    T result = (T) json.get(fieldName);
    if (result == null) {
      throw new IOException("Missing required field '" + fieldName + "'");
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static String nonRequired(GenericJson json, String fieldName) throws IOException {
    return (String) json.get(fieldName);
  }

  @SuppressWarnings("unchecked")
  private static Integer nonRequiredInt(GenericJson json, String fieldName) throws IOException {
    Object result = json.get(fieldName);
    if (result == null) {
      return null;
    }

    if (result.getClass() == Integer.class) {
      return (Integer) result;
    }
    if (result.getClass() == BigDecimal.class) {
      return ((BigDecimal) result).intValue();
    }
    if (result.getClass() == String.class) {
      return Integer.valueOf((String) result);
    }
    throw new IOException("Unable to convert " + result.getClass() + " to Integer");
  }
}
