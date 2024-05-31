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

import java.io.Serializable;
import java.util.Objects;

public class ProcessInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String processName;
  private String category;

  public String getProcessName() {
    return processName;
  }

  public String getCategory() {
    return category;
  }

  public static ProcessInfo create(String processName, String category) {
    ProcessInfo result = new ProcessInfo();
    result.processName = processName;
    result.category = category;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessInfo that = (ProcessInfo) o;
    return processName.equals(that.processName) &&
        category.equals(that.category);
  }

  @Override
  public int hashCode() {
    return Objects.hash(processName, category);
  }
}
