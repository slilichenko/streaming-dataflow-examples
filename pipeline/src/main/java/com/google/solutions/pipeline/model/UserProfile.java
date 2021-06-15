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
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class UserProfile {
  public enum Status {regular, suspicious}
  public abstract Status getStatus();

  public static Builder builder() {
    return new AutoValue_UserProfile.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStatus(Status newStatus);

    public abstract UserProfile build();
  }
}
