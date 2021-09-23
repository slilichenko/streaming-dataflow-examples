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
import com.google.solutions.pipeline.model.UserEventFinding;
import org.apache.beam.sdk.transforms.DoFn;

public class UserEventFindingToTableRow extends DoFn<UserEventFinding, TableRow> {

  private final static long serialVersionUID = 1L;

  @ProcessElement
  public void process(@Element UserEventFinding event, OutputReceiver<TableRow> out) {
    TableRow row = new TableRow();
    row.set("request_ts", event.getRequestTime().toString());
    row.set("type", "user-event-finding");
    row.set("level", event.getFinding().getLevel().toString());
    row.set("source_ip", event.getSourceIp());
    row.set("description", event.getFinding().getDescription());
    row.set("user_id", event.getUserId());

    out.output(row);
  }
}
