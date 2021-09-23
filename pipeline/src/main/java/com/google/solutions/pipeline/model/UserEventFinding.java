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
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class UserEventFinding {
  public abstract Instant getRequestTime();
  public abstract String getUserId();
  public abstract Finding getFinding();
  public abstract String getSourceIp();

  public static UserEventFinding create(Instant newRequestTime, String newUserId,
      Finding newFinding,
      String newSourceIp) {
    return builder()
        .setRequestTime(newRequestTime)
        .setUserId(newUserId)
        .setFinding(newFinding)
        .setSourceIp(newSourceIp)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_UserEventFinding.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRequestTime(Instant newRequestTime);

    public abstract Builder setUserId(String newUserId);

    public abstract Builder setFinding(Finding newFinding);

    public abstract Builder setSourceIp(String newSourceIp);

    public abstract UserEventFinding build();
  }
}
