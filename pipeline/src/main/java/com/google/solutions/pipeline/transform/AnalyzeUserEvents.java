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

package com.google.solutions.pipeline.transform;

import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.UserEventFinding;
import com.google.solutions.pipeline.rule.user.UserEventRule;
import java.util.Set;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class AnalyzeUserEvents extends
    PTransform<PCollection<Event>, PCollection<UserEventFinding>> {

  private final static long serialVersionUID = 1L;

  private PCollectionView<Set<UserEventRule>> userEventRules;

  public AnalyzeUserEvents(PCollectionView<Set<UserEventRule>> userEventRules) {
    this.userEventRules = userEventRules;
  }

  @Override
  public PCollection<UserEventFinding> expand(PCollection<Event> canonicalEvents) {
    try {
      PCollection<KV<String, Event>> eventsByUserId = canonicalEvents
          .apply("Filter Events Without UserId", Filter.by(e -> e.getUserId() != null))
          .apply("Key By UserId",
              WithKeys.of((SerializableFunction<Event, String>) event -> event.getUserId()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(),
              canonicalEvents.getPipeline().getCoderRegistry().getCoder(Event.class)))
          .apply("Global Window with Immediate Triggering",
              Window.<KV<String, Event>>into(new GlobalWindows())
                  .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                  .discardingFiredPanes());

      return eventsByUserId
          .apply("Supply User Event Rules", ParDo.of(new RulesSupplier(userEventRules)).withSideInputs(userEventRules))
          .apply("Apply User Event Rules", ParDo.of(new UserEventRuleRunner(userEventRules)));
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(e);
    }
  }

  public static class RulesSupplier extends
      DoFn<KV<String, Event>, KV<String, Pair<Event, Set<UserEventRule>>>> {
    private static final long serialVersionUID = 1L;
    private PCollectionView<Set<UserEventRule>> userEventRules;

    public RulesSupplier(PCollectionView<Set<UserEventRule>> userEventRules) {
      this.userEventRules = userEventRules;
    }

    @ProcessElement
    public void process(@Element KV<String, Event> element, ProcessContext processContext,
        OutputReceiver<KV<String, Pair<Event, Set<UserEventRule>>>> outputReceiver) {
      var rules = processContext.sideInput(userEventRules);
      outputReceiver.output(KV.of(element.getKey(), Pair.of(element.getValue(), rules)));
    }
  }
}
