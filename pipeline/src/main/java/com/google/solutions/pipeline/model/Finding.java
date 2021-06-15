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

package com.google.solutions.pipeline.model;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class Finding {
  public static enum Level {warning, severe, critical}

  public abstract Level getLevel();
  public abstract String getRuleId();
  public abstract String getDescription();

  public static Finding create(Level newLevel, String newRuleId, String newDescription) {
    return builder()
        .setLevel(newLevel)
        .setRuleId(newRuleId)
        .setDescription(newDescription)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_Finding.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLevel(Level newLevel);

    public abstract Builder setRuleId(String newRuleId);

    public abstract Builder setDescription(String newDescription);

    public abstract Finding build();
  }
}
