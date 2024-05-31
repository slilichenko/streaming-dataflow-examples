package com.google.solutions.pipeline;


import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This example is taken directly from the Beam documentation about side-loading something every 5 seconds:
 * https://beam.apache.org/documentation/patterns/side-inputs/
 *
 * but it appears only loads the side-input every minute.
 *
 * The example can be run like this:
 *
 * mvn compile exec:java \
 *     -Dexec.mainClass=org.apache.beam.examples.DocumentationExample \
 *     -Dexec.cleanupDaemonThreads=false \
 *     -Pdataflow-runner \
 *     -Dexec.args=" \
 *         --project=${GCP_PROJECT} \
 *         --region=${GCP_REGION} \
 *         --gcpTempLocation=${TEMP_LOCATION} \
 *         --runner=DataflowRunner \
--enableStreamingEngine\
 *         --serviceAccount=${SVC_ACC} \
 *         --network=${VPC_NAME} \
 *         --subnetwork=https://www.googleapis.com/compute/v1/projects/${SUBNET_PATH} \
 *         --usePublicIps=false \
 *         --dataflowKmsKey=${KEY_PATH}"
 *
 */
public class StreamingSideInputCachingPipeline implements Serializable {

  Logger LOG = LoggerFactory.getLogger(StreamingSideInputCachingPipeline.class);

  /*
   * Define your own configuration options. Add your own arguments to be processed
   * by the command-line parser, and specify default values for them.
   */
  public interface MyCustomOptions extends DataflowStreamingPipelineOptions, StreamingOptions {

  }

  public static void main(String[] args) throws IOException {
    StreamingSideInputCachingPipeline example = new StreamingSideInputCachingPipeline();
    example.runPipeline(args);
  }

  public void runPipeline(String[] args) throws IOException {
    MyCustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(MyCustomOptions.class);
    options.setStreamingSideInputCacheExpirationMillis(1000);
    Pipeline p = Pipeline.create(options);

    // Create a side input that updates every 5 seconds.
    // View as an iterable, not singleton, so that if we happen to trigger more
    // than once before Latest.globally is computed we can handle both elements.
    PCollectionView<Iterable<Map<String, String>>> mapIterable =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(5L)))
            .apply(
                ParDo.of(
                    new DoFn<Long, Map<String, String>>() {

                      @ProcessElement
                      public void process(
                          @Element Long input,
                          @Timestamp Instant timestamp,
                          OutputReceiver<Map<String, String>> o) {
                        // Replace map with test data from the placeholder external service.
                        // Add external reads here.
                        o.output(PlaceholderExternalService.readTestData(timestamp));
                      }
                    }))
            .apply(
                Window.<Map<String, String>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes())
            .apply(Latest.globally())
            .apply(View.asIterable());

    // Consume side input. GenerateSequence generates test data.
    // Use a real source (like PubSubIO or KafkaIO) in production.
    p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1L)))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
        .apply(Sum.longsGlobally().withoutDefaults())
        .apply(
            ParDo.of(
                    new DoFn<Long, KV<Long, Long>>() {

                      @ProcessElement
                      public void process(ProcessContext c, @Timestamp Instant timestamp) {
                        Iterable<Map<String, String>> si = c.sideInput(mapIterable);
                        // Take an element from the side input iterable (likely length 1)
                        Map<String, String> keyMap = si.iterator().next();
                        c.outputWithTimestamp(KV.of(1L, c.element()), Instant.now());

                        LOG.info(
                            "Value is {} with timestamp {}, using key A from side input with time {}.",
                            c.element(),
                            timestamp.toString(DateTimeFormat.forPattern("HH:mm:ss")),
                            keyMap.get("Key_A"));
                      }
                    })
                .withSideInputs(mapIterable));

    p.run();
  }

  /**
   * Placeholder class that represents an external service generating test data.
   */
  public static class PlaceholderExternalService {

    public static Map<String, String> readTestData(Instant timestamp) {
      Map<String, String> map = new HashMap<>();
      map.put("Key_A", timestamp.toString(DateTimeFormat.forPattern("HH:mm:ss")));
      return map;
    }
  }
}
