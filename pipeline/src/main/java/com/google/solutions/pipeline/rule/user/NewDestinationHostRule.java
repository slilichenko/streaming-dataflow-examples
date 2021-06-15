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

package com.google.solutions.pipeline.rule.user;

import com.google.solutions.pipeline.model.Finding;
import com.google.solutions.pipeline.model.Finding.Level;
import java.util.Collections;
import java.util.Set;

public class NewDestinationHostRule implements UserEventRule {
  private static final long serialVersionUID = 1L;

  public static final String RULE_ID = "new-destination-host";

  @Override
  public String getRuleId() {
    return RULE_ID;
  }

  @Override
  public Set<Finding> analyze(UserEventContext context) {
    return context.isNewlyAccessedHost() ?
        Collections.singleton(Finding.create(Level.warning, RULE_ID,
            "Newly accessed host: " + context.getEvent().getDestinationIp()))
        :
            Collections.EMPTY_SET;
  }
}
