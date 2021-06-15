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
import com.google.solutions.pipeline.model.SourceIpFinding;
import com.google.solutions.pipeline.rule.sourceip.BannedProcessDetector;
import com.google.solutions.pipeline.rule.sourceip.SessionAnalysisRule;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class AnalizeSourceIpEvents extends
    PTransform<PCollection<Event>, PCollection<SourceIpFinding>> {
  private final static long serialVersionUID = 1L;

  private Set<String> defaultMalwareProcessNames;
  private int sessionWindowGapInSeconds = 60;

  public AnalizeSourceIpEvents(Set<String> defaultMalwareProcessNames) {
    this.defaultMalwareProcessNames = defaultMalwareProcessNames;
  }

  @Override
  public PCollection<SourceIpFinding> expand(PCollection<Event> input) {
    // Analysis of the event stream by grouping events by source IP...
    PCollection<KV<String, Event>> eventsBySourceIp = null;
    try {
      eventsBySourceIp = input.apply("Key by Source IP",
          WithKeys.of((SerializableFunction<Event, String>) event -> event.getSourceIp()))
          .setCoder(KvCoder.of(StringUtf8Coder.of(),
              input.getPipeline().getCoderRegistry().getCoder(Event.class)));
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(e);
    }

    // ... and then by session (events with the same source IP occurred within a minute)
    PCollection<KV<String, Iterable<Event>>> eventsInSession = eventsBySourceIp
        .apply(sessionWindowGapInSeconds + " Second Window",
            Window.<KV<String, Event>>into(Sessions.withGapDuration(
                Duration.standardSeconds(sessionWindowGapInSeconds)))
                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                .withAllowedLateness(Duration.standardHours(1))
                .discardingFiredPanes()
        )
        .apply("Group by Source IP", GroupByKey.create());

    // ... finally, detecting and capturing suspicious activity to be persisted/processed
    // in subsequent step(s).

    // Rules are hard coded in this example. See AnalyzeUserEvents for more dynamic rule creation.
    Set<SessionAnalysisRule> rules = new HashSet<>();
    rules.add(new BannedProcessDetector(defaultMalwareProcessNames));

    return eventsInSession.apply(ParDo.of(new SourceIpSessionAnalyzer(rules)));
  }
}
