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
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class Event implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract Instant getRequestTime();

  public abstract String getSourceIp();

  public abstract String getDestinationIp();

  public abstract Integer getDestinationPort();

  @Nullable
  public abstract String getProcessName();

  @Nullable
  public abstract String getUserId();

  public abstract Integer getBytesSent();

  public abstract Integer getBytesReceived();

  public static Builder builder() {
    return new AutoValue_Event.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setRequestTime(Instant newRequestTime);

    public abstract Builder setSourceIp(String newSourceIp);

    public abstract Builder setDestinationIp(String newDestinationIp);

    public abstract Builder setDestinationPort(Integer newDestinationPort);

    public abstract Builder setProcessName(String newProcessName);

    public abstract Builder setUserId(String newUserId);

    public abstract Builder setBytesSent(Integer newBytesSent);

    public abstract Builder setBytesReceived(Integer newBytesReceived);

    public abstract Event build();
  }
}
