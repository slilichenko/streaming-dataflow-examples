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

import com.google.solutions.pipeline.EventProcessingPipeline;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.Finding;
import com.google.solutions.pipeline.model.HostSession;
import com.google.solutions.pipeline.model.SourceIpFinding;
import com.google.solutions.pipeline.rule.sourceip.SessionAnalysisRule;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;

public class SourceIpSessionAnalyzer extends DoFn<KV<String, Iterable<Event>>, SourceIpFinding> {

  private final static long serialVersionUID = 1L;

  private Set<SessionAnalysisRule> rules;

  public SourceIpSessionAnalyzer(Set<SessionAnalysisRule> rules) {
    this.rules = rules;
  }

  @ProcessElement
  public void process(@Element KV<String, Iterable<Event>> eventsBySourceIp,
      OutputReceiver<SourceIpFinding> out, BoundedWindow window) {
    String sourceIp = eventsBySourceIp.getKey();
    HostSession session = HostSession.create(eventsBySourceIp.getValue(), sourceIp);

    rules.forEach(rule -> {
      Set<Finding> findings = rule.getFindings(session);
      if (findings.size() > 0) {
        EventProcessingPipeline.suspiciousActivity.inc(findings.size());
        findings.forEach(
            finding -> out.output(
                // TODO: max timestamp is just one option.
                SourceIpFinding.create(window.maxTimestamp(), eventsBySourceIp.getKey(), finding)));
      }
    });
  }
}
