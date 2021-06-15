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

package com.google.solutions.pipeline;

import static com.google.solutions.pipeline.EventProcessingPipeline.PipelineInputs;
import static com.google.solutions.pipeline.EventProcessingPipeline.mainProcessing;

import com.google.solutions.pipeline.EventProcessingPipeline.PipelineOutputs;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.Finding;
import com.google.solutions.pipeline.model.Finding.Level;
import com.google.solutions.pipeline.model.InvalidPubSubMessage;
import com.google.solutions.pipeline.model.ProcessInfo;
import com.google.solutions.pipeline.model.SourceIpFinding;
import com.google.solutions.pipeline.model.UserEventFinding;
import com.google.solutions.pipeline.rule.user.NewDestinationHostRule;
import com.google.solutions.pipeline.rule.user.UserEventRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class EventProcessingPipelineTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void pubsubFlow() throws CannotProvideCoderException {
    Instant eventPublishTime = new Instant(0L);

    PipelineInputs inputs = new PipelineInputs();
    String malwareProcess = "virus1";
    String user = "user1";
    inputs.defaultMalwareProcessNames = Collections.singleton(malwareProcess);

    PubsubMessage invalidMessage = ConversionUtils.pubsubMessage("junk");
    PubsubMessage[] validMessages = new PubsubMessage[]{
        ConversionUtils.validPubsubMessage(eventPublishTime, "1.1.1.1", "2.2.2.2", 80, 20, 30,
            "process1", user),
        ConversionUtils.validPubsubMessage(eventPublishTime, "1.1.1.1", "2.2.2.3", 80, 20, 30,
            malwareProcess, user)
    };

    Set<Event> expectedEvents = new HashSet<>();
    Arrays.stream(validMessages).forEach(m -> {
      expectedEvents
          .add(ConversionUtils.fromString(new String(m.getPayload()), getProcessMetadata()));
    });

    Builder<PubsubMessage> messageFlow = TestStream
        .create(PubsubMessageWithAttributesAndMessageIdCoder.of())
        .advanceWatermarkTo(eventPublishTime);

    messageFlow = messageFlow.addElements(invalidMessage);
    for (PubsubMessage m : validMessages) {
      messageFlow =
          messageFlow
              .advanceWatermarkTo(eventPublishTime.plus(Duration.standardSeconds(1)))
              .addElements(m);
    }

    // Needed to force the processing time based timers.
    messageFlow = messageFlow.advanceProcessingTime(Duration.standardMinutes(5));

    inputs.rawPubSubMessages = testPipeline
        .apply("Create Events", messageFlow.advanceWatermarkToInfinity());

    inputs.processMetadata = prepareProcessMetadata(testPipeline);
    inputs.userEventRules = prepareUserEventRules(testPipeline);
    PipelineOutputs outputs = mainProcessing(testPipeline,
        testPipeline.getOptions().as(EventProcessingPipelineOptions.class), inputs);

    // Check canonical events
    PAssert.that("Canonical events", outputs.canonicalEvents).satisfies(events -> {
      int count = 0;

      for (Event event : events) {
        Assert.assertTrue("Event exists", expectedEvents.contains(event));
        count++;
      }
      Assert.assertEquals("Canonical message count", expectedEvents.size(), count);
      return null;
    });

    // Check failed events
    PAssert.that("Failed pubsub events", outputs.invalidPubSubMessages).containsInAnyOrder(
        InvalidPubSubMessage.create(
            "Failed to parse: Unrecognized token 'junk': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n"
                + " at [Source: (ByteArrayInputStream); line: 1, column: 5]", invalidMessage,
            eventPublishTime)
    );

    // Check source ip findings
//    PAssert.that("Source ip findings", outputs.sourceIpFindings)
//        .containsInAnyOrder(
//            SourceIpFinding
//                .create(eventPublishTime.plus(Duration.standardMinutes(1)),"1.1.1.1", Finding.create(Level.severe, "banned-process-detector",
//                    "Found banned processes: " + malwareProcess)));

    // Check user id findings
    PAssert.that("User id findings", outputs.userEventFindings).containsInAnyOrder(
        UserEventFinding.create(eventPublishTime, user,
            Finding.create(Level.warning, NewDestinationHostRule.RULE_ID,
                "Newly accessed host: 2.2.2.2")),
        UserEventFinding.create(eventPublishTime, user,
            Finding.create(Level.warning, NewDestinationHostRule.RULE_ID,
                "Newly accessed host: 2.2.2.3"))
    );

    testPipeline.run();
  }

  private static PCollectionView<Map<String, ProcessInfo>> prepareProcessMetadata(Pipeline p) {
    return p.apply("Sequence of 1", GenerateSequence.from(0).to(1))
        .apply("GlobalWindowForProcessMetadata", Window.<Long>into(new GlobalWindows())
            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
            .discardingFiredPanes())
        .apply("FetchProcessMetadata", ParDo.of(
            new DoFn<Long, Map<String, ProcessInfo>>() {
              private static final long serialVersionUID = 1l;

              @ProcessElement
              public void process(OutputReceiver<Map<String, ProcessInfo>> o) {
                o.output(getProcessMetadata());
              }
            }
        ))
        .apply("ProcessMetadataAsSingleton", View.asSingleton());
  }

  private static PCollectionView<Set<UserEventRule>> prepareUserEventRules(Pipeline p) {
    return
        p.apply("SequenceEvery30Sec",
            GenerateSequence.from(0).to(1))
            .apply("GlobalWindowForUserEventRules", Window.<Long>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                .discardingFiredPanes())
            .apply("FetchUserEventRules",
                ParDo.of(new DoFn<Long, Set<UserEventRule>>() {
                           @ProcessElement
                           public void instantiateRuleSet(OutputReceiver<Set<UserEventRule>> out) {
                             // TODO: externalize rule configuration
                             Set<UserEventRule> rules = new HashSet<>();
                             rules.add(new NewDestinationHostRule());

                             out.output(rules);
                           }

                         }
                ))
            .apply("UserEventRulesAsSingleton", View.asSingleton());
  }

  private static Map<String, ProcessInfo> getProcessMetadata() {
    return Collections.EMPTY_MAP;
  }

}