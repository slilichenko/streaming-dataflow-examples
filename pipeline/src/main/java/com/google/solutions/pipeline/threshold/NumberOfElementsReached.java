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

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Outputs a single value once the number of elements in the input collection reached the threshold
 *
 * @param <InputT> Input PCollection type
 * @param <OutputT> Output PCollection type
 */
public class NumberOfElementsReached<InputT, OutputT> extends
    PTransform<PCollection<InputT>, PCollection<OutputT>> {

  public static final org.apache.beam.sdk.metrics.Counter numberOfElementsMetric = Metrics
      .counter(NumberOfElementsReached.class, "number-of-elements");

  private final static long serialVersionUID = 1L;
  private final long threshold;
  private final OutputT outputValue;

  /**
   * Creates the transform
   *
   * @param threshold after which the outputValue will be sent
   * @param outputValue
   */
  public NumberOfElementsReached(long threshold, OutputT outputValue) {
    this.threshold = threshold;
    this.outputValue = outputValue;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> input) {
    Coder<OutputT> coder;
    try {
      coder = (Coder<OutputT>) input.getPipeline().getCoderRegistry().getCoder(outputValue.getClass());
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Unable to determine coder for " + outputValue.getClass());
    }
    return input.apply("Into Global Window", Window.into(new GlobalWindows()))
        .apply("Map to KV", MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.booleans(), TypeDescriptors.booleans()
            )).via(value -> KV.of(Boolean.TRUE, Boolean.TRUE)))
        .apply("Count and Output", ParDo.of(new Counter<>(threshold, outputValue)))
        .setCoder(coder);
  }

  /**
   * Counter class to do counting of elements. Needs to have KV as input because it uses State,
   * which only works with KVs. Because only counting is needed the source collection is converted
   * to KV&lt;Boolean,Boolean&gt; as hopefully the smallest KV to pass in order to minimize the
   * performance hit due to a single threaded counting of elements.
   *
   * Alternative to consider - Count.globally().
   * @param <OutputT>
   */
  static class Counter<OutputT> extends DoFn<KV<Boolean, Boolean>, OutputT> {

    private final static long serialVersionUID = 1L;

    private final long threshold;
    private final OutputT output;

    @StateId("number-of-elements")
    private final StateSpec<ValueState<Long>> counterSpec = StateSpecs.value();

    public Counter(long threshold, OutputT output) {
      this.threshold = threshold;
      this.output = output;
    }

    @ProcessElement
    public void count(@StateId("number-of-elements") ValueState<Long> counterState,
        OutputReceiver<OutputT> outputReceiver) {
      numberOfElementsMetric.inc();

      Long numberOfElements = counterState.read();
      long newNumberOfElements = numberOfElements == null ? 1 : ++numberOfElements;

      if (newNumberOfElements > threshold) {
        // nothing to do - we already produced the element that was needed.
        return;
      }
      counterState.write(newNumberOfElements);
      if (newNumberOfElements == threshold) {
        outputReceiver.output(output);
      }
    }
  }
}
