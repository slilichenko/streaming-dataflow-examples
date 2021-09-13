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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class NumberOfElementsReachedTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testTransform() {
    long middleThreshold = 3;
    long endThreshold = 5;
    long pastEndThreshold = 15;
    PCollection<Integer> input = testPipeline.apply("Sequence", Create.of(1, 2, 3, 4, 5));

    PCollection<String> middle = input
        .apply("Check middle", new NumberOfElementsReached(middleThreshold));
    PAssert.that(middle).containsInAnyOrder("Number of elements reached " + middleThreshold);

    PCollection<String> end = input
        .apply("Check end", new NumberOfElementsReached(endThreshold));
    PAssert.that(end).containsInAnyOrder("Number of elements reached " + endThreshold);

    PCollection<String> pastEnd = input
        .apply("Check past end", new NumberOfElementsReached(pastEndThreshold));
    PAssert.that(pastEnd).empty();

    testPipeline.run().waitUntilFinish();
  }
}