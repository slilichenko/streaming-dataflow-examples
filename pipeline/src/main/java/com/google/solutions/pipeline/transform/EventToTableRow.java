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
import com.google.solutions.pipeline.model.Event;
import org.apache.beam.sdk.transforms.DoFn;

public class EventToTableRow extends DoFn<Event, TableRow> {

  private final static long serialVersionUID = 1L;

  @ProcessElement
  public void process(@Element Event event, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    row.set("request_ts", event.getRequestTime().toString());
    row.set("dst_ip", event.getDestinationIp());
    row.set("dst_port", event.getDestinationPort());
    row.set("src_ip", event.getSourceIp());
    row.set("bytes_sent", event.getBytesSent());
    row.set("bytes_received", event.getBytesReceived());
    row.set("user_id", event.getUserId());
    row.set("process_name", event.getProcessName());

    out.output(row);
  }
}
