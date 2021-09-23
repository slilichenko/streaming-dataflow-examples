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


import com.google.api.client.json.GenericJson;
import com.google.api.client.json.gson.GsonFactory;
import com.google.solutions.pipeline.model.Event;
import com.google.solutions.pipeline.model.InvalidPubSubMessage;
import com.google.solutions.pipeline.model.ProcessInfo;
import com.google.solutions.pipeline.model.SourceIpFinding;
import com.google.solutions.pipeline.model.UserEventFinding;
import com.google.solutions.pipeline.rule.user.NewDestinationHostRule;
import com.google.solutions.pipeline.rule.user.ProcessNameRule;
import com.google.solutions.pipeline.rule.user.UserEventRule;
import com.google.solutions.pipeline.threshold.NumberOfElementsReached;
import com.google.solutions.pipeline.transform.AnalizeSourceIpEvents;
import com.google.solutions.pipeline.transform.AnalyzeUserEvents;
import com.google.solutions.pipeline.transform.EventToTableRow;
import com.google.solutions.pipeline.transform.InvalidPubSubToTableRow;
import com.google.solutions.pipeline.transform.PubSubMessageToEvent;
import com.google.solutions.pipeline.transform.SourceIpFindingToJsonString;
import com.google.solutions.pipeline.transform.StopPipeline;
import com.google.solutions.pipeline.transform.StopPipeline.HowToStop;
import com.google.solutions.pipeline.transform.UserEventFindingToJSonString;
import com.google.solutions.pipeline.transform.UserEventFindingToTableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for the event processing pipeline.
 */
public class EventProcessingPipeline {

  public static final Logger LOG = LoggerFactory.getLogger(
      EventProcessingPipeline.class);

  private static final String ACCEPTED_FILE_PATTERN = "(^.*\\.(json|JSON)$)";

  public static final Counter totalEvents = Metrics
      .counter(EventProcessingPipeline.class, "totalEvents");
  public static final Counter rejectedEvents = Metrics
      .counter(EventProcessingPipeline.class, "rejectedEvents");
  public static final Counter rejectedFiles = Metrics
      .counter(EventProcessingPipeline.class, "rejectedFiles");
  public static final Counter suspiciousActivity = Metrics
      .counter(EventProcessingPipeline.class, "suspiciousActivity");

  @SuppressWarnings("serial")
  public final static TupleTag<Event> validEvents = new TupleTag<>() {
  };
  @SuppressWarnings("serial")
  public final static TupleTag<InvalidPubSubMessage> invalidPubSubMessages = new TupleTag<>() {
  };

  public static class PipelineInputs {

    PCollection<PubsubMessage> rawPubSubMessages;
    PCollection<PubsubMessage> anotherSourcePubSubMessages;
    PCollection<String> rawJsonMessages;
    PCollectionView<Map<String, ProcessInfo>> processMetadata;
    PCollectionView<Set<UserEventRule>> userEventRules;
    Set<String> defaultMalwareProcessNames = Collections.EMPTY_SET;
  }

  public static class PipelineOutputs {

    PCollection<PubsubMessage> rawPubSubMessages;
    PCollection<SourceIpFinding> sourceIpFindings;
    PCollection<Event> canonicalEvents;
    PCollection<InvalidPubSubMessage> invalidPubSubMessages;
    PCollection<UserEventFinding> userEventFindings;
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link EventProcessingPipeline#run(EventProcessingPipelineOptions)}
   * method to start the pipeline and invoke {@code result.waitUntilFinish()} on the {@link
   * PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) throws CannotProvideCoderException {
    EventProcessingPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(EventProcessingPipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline
   *
   * @return result
   */
  public static PipelineResult run(EventProcessingPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    PipelineInputs inputs = getPipelineInputs(options, pipeline);

    PipelineOutputs outputs = mainProcessing(inputs);

    Long maxMessagesToRead = options.getMaxMessagesToRead();
    if(maxMessagesToRead != null) {
      shutdownPipelineAfter(maxMessagesToRead.longValue(), inputs.rawPubSubMessages);
    }

    persistOutputs(outputs, options);

    return pipeline.run();
  }

  private static void shutdownPipelineAfter(long maxInputMessages,
      PCollection<PubsubMessage> inputMessages) {
    inputMessages
        .apply("Wait until " + inputMessages + " read", new NumberOfElementsReached<>(maxInputMessages, "read at least "
            + maxInputMessages + " messages"))
        .apply("Send cancel signal", ParDo.of(
            new DoFn<String, StopPipeline.StopInstruction>() {
              @ProcessElement
              public void sendInstruction(@Element String message,
                  OutputReceiver<StopPipeline.StopInstruction> outputReceiver) {
                outputReceiver
                    .output(StopPipeline.StopInstruction.create(HowToStop.cancel, message));
              }
            }
        ))
        .apply("Stop the pipeline", new StopPipeline());
  }

  private static PipelineInputs getPipelineInputs(EventProcessingPipelineOptions options,
      Pipeline pipeline) {

    PipelineInputs inputs = new PipelineInputs();

    if (options.getSubscriptionId() != null) {
      inputs.rawPubSubMessages = pipeline.begin().apply("Read PubSub",
          PubsubIO.readMessagesWithAttributesAndMessageId()
              .fromSubscription(options.getSubscriptionId())).setCoder(
          PubsubMessageWithAttributesAndMessageIdCoder.of());
    } else if (options.getFileList() != null) {
      PCollection<String> filesToProcess = listGCSFiles(pipeline, options);
      throw new IllegalStateException("Not implemented yet.");
// TODO: extract Events
    } else {
      throw new RuntimeException("Either the subscription id or the file list should be provided.");
    }

    if (options.getAnotherSubscriptionId() != null) {
      inputs.anotherSourcePubSubMessages = pipeline.begin().apply("Read Another PubSub",
          PubsubIO.readMessagesWithAttributesAndMessageId()
              .fromSubscription(options.getAnotherSubscriptionId()));
    }

    inputs.processMetadata =
        pipeline.apply("Provide Process Metadata",
            new ProcessMetadataSupplier(
                options.getDatasetName(),
                options.getProcessInfoTableName()));

    inputs.userEventRules = pipeline.apply("Provide User Event Rules",
        new PTransform<>() {
          @Override
          public PCollectionView<Set<UserEventRule>> expand(PBegin input) {
            return input.apply("Every 5 Minutes",
                GenerateSequence.from(0).withRate(1, Duration.standardMinutes(5L)))
                .apply("Global Window For User Event Rules", Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes())
                .apply("Fetch User Event Rule Configuration",
                    ParDo.of(new DoFn<Long, Set<UserEventRule>>() {
                               @ProcessElement
                               public void instantiateRuleSet(OutputReceiver<Set<UserEventRule>> out) {
                                 // TODO: externalize rule configuration
                                 Set<UserEventRule> rules = new HashSet<>();
                                 rules.add(new NewDestinationHostRule());
                                 rules.add(new ProcessNameRule());

                                 out.output(rules);
                               }

                             }
                    ))
                .apply(View.asSingleton());
          }
        });

    return inputs;
  }

  public static PipelineOutputs mainProcessing(
      PipelineInputs inputs) {
    PipelineOutputs outputs = new PipelineOutputs();
    outputs.rawPubSubMessages = inputs.rawPubSubMessages;

    // Convert raw inputs into canonical messages. Processing can vary slightly depending on the source.
    PCollection<Event> canonicalEvents;
    if (inputs.rawPubSubMessages != null) {
      PCollectionTuple extractedEvents = inputs.rawPubSubMessages.apply("Extract Event",
          ParDo.of(new PubSubMessageToEvent(inputs.processMetadata))
              .withOutputTags(validEvents, TupleTagList
                  .of(invalidPubSubMessages))
              .withSideInput("processMetadata", inputs.processMetadata)
      );
      canonicalEvents = extractedEvents.get(validEvents);

      outputs.invalidPubSubMessages = extractedEvents.get(invalidPubSubMessages);

      // Optionally combine events from several sources into a single stream of events
      if (inputs.anotherSourcePubSubMessages != null) {
        PCollectionTuple otherEvents = inputs.anotherSourcePubSubMessages
            .apply("Extract Other Event",
                ParDo.of(new PubSubMessageToEvent(inputs.processMetadata))
                    .withOutputTags(validEvents, TupleTagList
                        .of(invalidPubSubMessages))
                    .withSideInputs(inputs.processMetadata)
            );

        PCollectionList<Event> twoSources = PCollectionList.of(canonicalEvents)
            .and(otherEvents.get(validEvents));

        canonicalEvents = twoSources.apply(Flatten.pCollections());
      }
    } else if (inputs.rawJsonMessages != null) {
      throw new IllegalStateException("Not yet implemented");
    } else {
      throw new IllegalStateException("Either raw JSON or raw PubSub messages should be provided");
    }

    // Canonical events will be stored in a BigQuery table as-is
    outputs.canonicalEvents = canonicalEvents;

    outputs.userEventFindings = canonicalEvents
        .apply("Analyze User Events", new AnalyzeUserEvents(inputs.userEventRules));

    outputs.sourceIpFindings = canonicalEvents
        .apply("Analyze Events by Source IP",
            new AnalizeSourceIpEvents(inputs.defaultMalwareProcessNames));

    return outputs;
  }

  public static void persistOutputs(PipelineOutputs outputs,
      EventProcessingPipelineOptions options) {
    outputs.canonicalEvents.apply("Persist Events to BQ",
        new PersistEvents(options.getDatasetName(), options.getEventsTable()));

    final String gcsBucketEventExport = options.getGCSBucketEventExport();
    if (outputs.rawPubSubMessages != null && gcsBucketEventExport != null) {
      outputs.rawPubSubMessages
          .apply("PubSub to Text", ParDo.of(new DoFn<PubsubMessage, String>() {
            @ProcessElement
            public void extractText(@Element PubsubMessage message,
                OutputReceiver<String> outputReceiver) {
              GsonFactory factory = GsonFactory.getDefaultInstance();
              try {
                GenericJson json = factory
                    .createJsonParser(new ByteArrayInputStream(message.getPayload())).parse(
                        GenericJson.class);
                json.setFactory(factory);
                outputReceiver.output(json.toString());
              } catch (IOException e) {
                // Do nothing - parsing errors are captured in another branch of the pipeline
              }
            }
          }))
          .apply("Window into every minute",
              Window.into(FixedWindows.of(Duration.standardMinutes(1))))
          .apply("Persist Events to GCS", TextIO.write()
              .to(new FilenamePolicy() {
                @Override
                public ResourceId windowedFilename(int shardNumber, int numShards,
                    BoundedWindow window,
                    PaneInfo paneInfo, OutputFileHints outputFileHints) {

                  // TODO: needs to be set once per JVM.
                  DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd");
                  DateTimeFormatter hourFormat = DateTimeFormat.forPattern("HH");
                  DateTimeFormatter minuteFormat = DateTimeFormat.forPattern("mm");
                  Instant windowStart = ((IntervalWindow) window).start();
                  ResourceId file = FileSystems
                      .matchNewResource(gcsBucketEventExport + "/data/" +
                          "eventDate=" + dateFormat.print(windowStart) + '/' +
                          "eventHour=" + hourFormat.print(windowStart) + '/' +
                          "eventMinute=" + minuteFormat.print(windowStart) + '/' +
                          UUID.randomUUID() + ".json", false);
                  return file;
                }

                @Override
                public @Nullable ResourceId unwindowedFilename(int shardNumber, int numShards,
                    OutputFileHints outputFileHints) {
                  throw new UnsupportedOperationException("Not implemented");
                }
              })
              .withWindowedWrites()
              .withTempDirectory(FileBasedSink
                  .convertToFileResourceIfPossible(gcsBucketEventExport + "/temp"))
              .withNumShards(1));
    }

    outputs.invalidPubSubMessages.apply("Persist Invalid Messages",
        new InvalidMessageOutput(options.getDatasetName(), options.getInvalidMessagesTable()));

    outputs.sourceIpFindings.apply("Persist Source IP Findings",
        new PTransform<>() {
          @Override
          public POutput expand(PCollection<SourceIpFinding> input) {
            return input.apply(
                "Source IP Finding to JSon", ParDo.of(new SourceIpFindingToJsonString()))
                .apply("Source IP Findings to PubSub",
                    PubsubIO.writeStrings().to(options.getSuspiciousActivityTopic()));
          }
        });

    outputs.userEventFindings.apply("Send User Event Finding Notifications",
        new PTransform<>() {
          @Override
          public POutput expand(PCollection<UserEventFinding> input) {
            return input
                .apply("User Event Findings to JSon", ParDo.of(new UserEventFindingToJSonString()))
                .apply("Publish User Event Findings to PubSub",
                    PubsubIO.writeStrings().to(options.getSuspiciousActivityTopic()));
          }
        });

    outputs.userEventFindings.apply("Persist User Event Findings",
        new PTransform<>() {
          @Override
          public POutput expand(PCollection<UserEventFinding> input) {
            return input
                .apply("User Event Findings to Row", ParDo.of(new UserEventFindingToTableRow()))
                .apply("Save to BigQuery",
                    BigQueryIO.writeTableRows().to(
                        options.getDatasetName() + '.' + options.getUserEventFindingsTable())
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER));
          }
        });
  }

  /**
   * Reads the GCS buckets provided by {@link EventProcessingPipelineOptions#getFileList()}.
   *
   * The file list can contain multiple entries. Each entry can contain wildcards supported by
   * {@link FileIO#matchAll()}.
   *
   * Files are filtered based on their suffixes as defined in {@link EventProcessingPipeline#ACCEPTED_FILE_PATTERN}.
   *
   * @return PCollection of GCS URIs
   */
  static PCollection<String> listGCSFiles(Pipeline p, EventProcessingPipelineOptions options) {
    PCollection<String> imageFileUris;
    PCollection<Metadata> allFiles = p.begin()
        .apply("Get File List", Create.of(options.getFileList()))
        .apply("Match GCS Files", FileIO.matchAll());
    imageFileUris = allFiles.apply(ParDo.of(new DoFn<Metadata, String>() {
      private static final long serialVersionUID = 1L;

      @ProcessElement
      public void processElement(@Element Metadata metadata, OutputReceiver<String> out) {
        out.output(metadata.resourceId().toString());
      }
    }))
        .apply("Filter out non-JSON files",
            Filter.by((SerializableFunction<String, Boolean>) fileName -> {
              totalEvents.inc();
              if (fileName.matches(ACCEPTED_FILE_PATTERN)) {
                return true;
              }
              LOG.warn("File {} does not contain a valid extension", fileName);
              rejectedFiles.inc();
              return false;
            }));
    return imageFileUris;
  }

  private static class InvalidMessageOutput extends
      PTransform<PCollection<InvalidPubSubMessage>, POutput> {

    private final static long serialVersionUID = 1L;

    private String invalidMessagesTable;
    private String datasetName;

    public InvalidMessageOutput(String datasetName, String invalidMessagesTable) {
      this.datasetName = datasetName;
      this.invalidMessagesTable = invalidMessagesTable;
    }

    @Override
    public POutput expand(PCollection<InvalidPubSubMessage> input) {

      return input
          .apply("Invalid PubSub to TableRow", ParDo.of(new InvalidPubSubToTableRow()))
          .apply("Save Invalid to BigQuery", BigQueryIO.writeTableRows()
              .to(datasetName + '.' + invalidMessagesTable)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withCreateDisposition(CreateDisposition.CREATE_NEVER));
    }
  }

  private static class ProcessMetadataSupplier extends
      PTransform<PBegin, PCollectionView<Map<String, ProcessInfo>>> {

    private static final long serialVersionUID = 1L;

    private String datasetName;
    private String processInfoTableName;

    public ProcessMetadataSupplier(String datasetName, String processInfoTableName) {
      this.datasetName = datasetName;
      this.processInfoTableName = processInfoTableName;
    }

    @Override
    public PCollectionView<Map<String, ProcessInfo>> expand(PBegin input) {
      return input.apply("Every 2 Minutes",
          GenerateSequence.from(0).withRate(1, Duration.standardMinutes(2L)))
          .apply("Global Window for Process Metadata",
              Window.<Long>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                  .discardingFiredPanes())
          .apply("Fetch Process Metadata", ParDo.of(
              new com.google.solutions.pipeline.transform.ProcessMetadataSupplier(
                  datasetName, processInfoTableName)
          ))
          .apply("Process Metadata as Singleton", View.asSingleton());
    }
  }

  private static class PersistEvents extends PTransform<PCollection<Event>, POutput> {

    private final static long serialVersionUID = 1L;

    private String datasetName;
    private String eventsTable;

    public PersistEvents(String datasetName, String eventsTable) {
      this.datasetName = datasetName;
      this.eventsTable = eventsTable;
    }

    @Override
    public POutput expand(PCollection<Event> input) {
      WriteResult writeResult = input
          .apply("Event to TableRow", ParDo.of(new EventToTableRow())).
              apply("Save Event to BigQuery", BigQueryIO.writeTableRows()
                  .to(datasetName + '.' + eventsTable)
                  .withExtendedErrorInfo()
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                  .withCreateDisposition(CreateDisposition.CREATE_NEVER));

      writeResult.getFailedInsertsWithErr()
          .apply("Every 2 Minutes", Window.into(FixedWindows.of(Duration.standardMinutes(2))))
          .apply("Process Insert Failure",
              ParDo.of(new DoFn<BigQueryInsertError, String>() {
                @ProcessElement
                public void process(@Element BigQueryInsertError error) {
                  LOG.error(
                      "Failed to insert: " + error.getError() + ", row: " + error.getRow());
                }
              })
          );
      return PDone.in(input.getPipeline());
    }
  }
}
