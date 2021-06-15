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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.solutions.pipeline.ConversionUtils;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.ProcessInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Instant;
import org.junit.Test;

public class ProxyRequestConverterTest {

  private Map<String, ProcessInfo> processMetadata = Collections
      .singletonMap("category", ProcessInfo.create("hostservice", "KNOWN"));

  @Test
  public void exceptionThrownIfInvalidJSON() throws IOException {
    GenericJson json = new JacksonFactory().createJsonParser("{}").parse(GenericJson.class);

    Throwable t = assertThrows(IOException.class,
        () -> new ProxyRequestConverter(processMetadata).convert(json));
    assertEquals("Missing required field 'destination_ip'", t.getMessage());
  }

  @Test
  public void validateConversion() throws IOException {
    Instant now = Instant.now();
    String sourceIp = "1.2.3.4";
    String destinationIp = "5.6.7.8";
    int destinationPort = 80;
    int bytesSent = 45;
    int bytesReceived = 67;
    String processName = "hostprocess.exe";
    String user = "user1";
    PubsubMessage pubsubMessage = ConversionUtils.validPubsubMessage(now, sourceIp, destinationIp,
        destinationPort, bytesSent, bytesReceived, processName, user);
    Event event = ConversionUtils.fromString(new String(pubsubMessage.getPayload(), "UTF-8"), Collections.EMPTY_MAP);
    assertEquals("source ip", sourceIp, event.getSourceIp());
    assertEquals("destination ip", destinationIp, event.getDestinationIp());
    assertEquals("destination port", destinationPort, (long)event.getDestinationPort());
    assertEquals("bytes sent", bytesSent, (long)event.getBytesSent());
    assertEquals("bytes received", bytesReceived, (long)event.getBytesReceived());
    assertEquals("request time", now, event.getRequestTime());
    assertEquals("process name", processName, event.getProcessName());
    assertEquals("user", user, event.getUserId());
  }

}