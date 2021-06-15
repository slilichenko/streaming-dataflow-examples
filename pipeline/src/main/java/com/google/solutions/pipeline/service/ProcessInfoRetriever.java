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

package com.google.solutions.pipeline.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.solutions.pipeline.model.ProcessInfo;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessInfoRetriever {

  public static final Logger LOG = LoggerFactory.getLogger(ProcessInfoRetriever.class);

  public Map<String, ProcessInfo> getProcessInfo(String datasetName, String tableName)
      throws InterruptedException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//    TODO: SQL Injection bug. Dataset name and table name should be checked to be valid ids.
    String query = "SELECT process_name, category FROM `"
        + datasetName + '.' + tableName + "`;";
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

    Map<String, ProcessInfo> result = new HashMap<>();
    for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
      ProcessInfo processInfo = ProcessInfo.create(
          row.get("process_name").getStringValue(),
          row.get("category").getStringValue());

      result.put(processInfo.getProcessName(), processInfo);
    }
    LOG.info("Retrieved " + result.size() + " process metadata records." );
    return result;
  }
}
