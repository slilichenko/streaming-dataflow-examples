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

package com.google.solutions.pipeline.rule.sourceip;

import com.google.solutions.pipeline.model.Finding;
import com.google.solutions.pipeline.model.Finding.Level;
import com.google.solutions.pipeline.model.HostSession;
import java.util.Collections;
import java.util.Set;

public class BannedProcessDetector implements SessionAnalysisRule {
  private static final long serialVersionUID = 1L;

  private final Set<String> bannedProcesses;

  public BannedProcessDetector(Set<String> bannedProcesses) {
    this.bannedProcesses = bannedProcesses;
  }

  @Override
  public String getRuleId() {
    return "banned-process-detector";
  }

  @Override
  public Set<Finding> getFindings(HostSession session) {
    StringBuilder detected = new StringBuilder();
    session.getEvents().forEach(e -> {
      if (bannedProcesses.contains(e.getProcessName())) {
        if (detected.length() > 0) {
          detected.append(", ");
        }
        detected.append(e.getProcessName());
      }
    });
    return detected.length() == 0 ?
        Collections.emptySet() :
        Collections.singleton(
            Finding.create(Level.severe, getRuleId(), "Found banned processes: " + detected));
  }
}
