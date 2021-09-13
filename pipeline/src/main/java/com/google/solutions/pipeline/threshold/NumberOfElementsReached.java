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

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TypeDescriptors;

public class NumberOfElementsReached extends
    PTransform<PCollection<Integer>, PCollection<String>> {

  private final long threshold;

  public NumberOfElementsReached(long threshold) {
    this.threshold = threshold;
  }


  @Override
  public PCollection<String> expand(PCollection<Integer> input) {
    return input.apply("Into Global Window", Window.into(new GlobalWindows()))
        .apply("Map to KV", MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.booleans(), TypeDescriptors.booleans()
            )).via(value -> KV.of(Boolean.TRUE, Boolean.TRUE)))
        .apply("Count", ParDo.of(new Counter(threshold)));
  }

  static class Counter extends DoFn<KV<Boolean, Boolean>, String> {

    private final long threshold;
    @StateId("number-of-elements")
    private final StateSpec<ValueState<Long>> counterSpec = StateSpecs.value();
    @StateId("threshold-reached")
    private final StateSpec<ValueState<Boolean>> thresholdReachedSpec = StateSpecs.value();

    public Counter(long threshold) {
      this.threshold = threshold;
    }

    @ProcessElement
    public void count(@StateId("number-of-elements") ValueState<Long> counterState,
        OutputReceiver<String> outputReceiver) {

      Long numberOfElements = counterState.read();
      long newNumberOfElements = numberOfElements == null ? 1 : numberOfElements + 1;

      if (newNumberOfElements > threshold) {
        // nothing to do - we already produced the element that was needed.
        return;
      }
      if (newNumberOfElements == threshold) {
        outputReceiver.output("Number of elements reached " + threshold);
      }
      counterState.write(Long.valueOf(newNumberOfElements));
    }
  }
}
