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

package com.google.solutions.pipeline.threshold;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class NumberOfElementsReachedTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testNumberOfElementsReached() {

    long elementCount = 25;

    PCollection<Long> input = testPipeline
        .apply("Sequence", GenerateSequence.from(0).to(elementCount));

    PAssert.that(
        input.apply("Check middle",
            new NumberOfElementsReached<>(elementCount - 2, "Reached middle"))
            .setCoder(StringUtf8Coder.of())
    ).containsInAnyOrder("Reached middle");

    PAssert.that(
        input.apply("Check end", new NumberOfElementsReached<>(elementCount, "Reached end"))
            .setCoder(StringUtf8Coder.of())
    ).containsInAnyOrder("Reached end");

    PAssert.that(
        input.apply("Check past end",
            new NumberOfElementsReached<>(elementCount + 1, "Should never reach"))
            .setCoder(StringUtf8Coder.of())
    ).empty();

    testPipeline.run().waitUntilFinish();
  }
}