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

import com.google.api.services.bigquery.model.TableRow;
import com.google.solutions.pipeline.model.InvalidPubSubMessage;
import java.io.UnsupportedEncodingException;
import org.apache.beam.sdk.transforms.DoFn;

public class InvalidPubSubToTableRow extends DoFn<InvalidPubSubMessage, TableRow> {
  private final static long serialVersionUID = 1L;

  @ProcessElement
  public void process(@Element InvalidPubSubMessage message, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    row.set("published_ts", message.getPublishTime().toString());
    row.set("message_id", message.getMessageId());
    row.set("payload_bytes", message.getPayload());
    try {
      row.set("payload_string", new String(message.getPayload(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Ignore
    }
    row.set("failure_reason", message.getFailureReason());
    out.output(row);
  }
}
