package com.hartwig.pipeline;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.api.HmfApi;
import com.hartwig.api.RunApi;
import com.hartwig.computeengine.execution.vm.GoogleComputeEngine;
import com.hartwig.computeengine.execution.vm.NoOpComputeEngine;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.events.EventContext;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pipeline.PipelineComplete;
import com.hartwig.events.pubsub.PubsubEventBuilder;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.cram.cleanup.CleanupProvider;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.input.InputMode;
import com.hartwig.pipeline.input.JsonPipelineInput;
import com.hartwig.pipeline.input.MetadataProvider;
import com.hartwig.pipeline.input.ModeResolver;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.metadata.HmfApiStatusUpdate;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.NoopOutputPublisher;
import com.hartwig.pipeline.output.OutputPublisher;
import com.hartwig.pipeline.output.PipelineCompleteEventPublisher;
import com.hartwig.pipeline.output.PipelineOutputComposerProvider;
import com.hartwig.pipeline.output.VmExecutionLogSummary;
import com.hartwig.pipeline.pubsub.PublisherProvider;
import com.hartwig.pipeline.reruns.InputPersistedDataset;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tools.VersionUtils;
import com.hartwig.pipeline.turquoise.Turquoise;

import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineMain.class);

    @NotNull
    private static IllegalStateException missingPubsubArgumentsException() {
        return new IllegalStateException("Cannot start pipeline with event publishing unless project, environment and workflow are defined");
    }

    private static SomaticPipeline somaticPipeline(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SomaticRunMetadata metadata, final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumourBamMetricsOutputQueue,
            final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue, final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue,
            final StartingPoint startingPoint, final PersistedDataset persistedDataset, final InputMode mode) throws Exception {
        final Labels labels = Labels.of(arguments, metadata);
        var computeEngineConfig = ArgumentUtil.toComputeEngineConfig(arguments);
        return new SomaticPipeline(arguments,
                new StageRunner<>(storage,
                        arguments,
                        arguments.publishEventsOnly() ? new NoOpComputeEngine() : GoogleComputeEngine.from(computeEngineConfig, credentials, labels.asMap()),
                        ResultsDirectory.defaultDirectory(),
                        startingPoint,
                        labels,
                        mode),
                referenceBamMetricsOutputQueue,
                tumourBamMetricsOutputQueue,
                referenceFlagstatOutputQueue,
                tumorFlagstatOutputQueue,
                metadata,
                PipelineOutputComposerProvider.from(storage, arguments, VersionUtils.pipelineVersion()).get(),
                Executors.newCachedThreadPool(),
                persistedDataset);
    }

    private static SingleSamplePipeline singleSamplePipeline(final Arguments arguments, final PipelineInput input,
            final GoogleCredentials credentials, final Storage storage, final SingleSampleEventListener eventListener,
            final SomaticRunMetadata metadata, final BlockingQueue<BamMetricsOutput> metricsOutputQueue,
            final BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue, final BlockingQueue<FlagstatOutput> flagstatOutputQueue,
            final StartingPoint startingPoint, final PersistedDataset persistedDataset, final InputMode mode) throws Exception {
        Labels labels = Labels.of(arguments, metadata);
        var computeEngineConfig = ArgumentUtil.toComputeEngineConfig(arguments);
        return new SingleSamplePipeline(eventListener,
                new StageRunner<>(storage,
                        arguments,
                        arguments.publishEventsOnly() ? new NoOpComputeEngine() : GoogleComputeEngine.from(computeEngineConfig, credentials, labels.asMap()),
                        ResultsDirectory.defaultDirectory(),
                        startingPoint,
                        labels,
                        mode),
                AlignerProvider.from(input, credentials, storage, arguments, labels).get(),
                PipelineOutputComposerProvider.from(storage, arguments, VersionUtils.pipelineVersion()).get(),
                Executors.newCachedThreadPool(),
                arguments,
                persistedDataset,
                metricsOutputQueue,
                flagstatOutputQueue,
                germlineCallerOutputQueue);
    }

    public static void main(final String[] args) {
        try {
            PipelineState state = new PipelineMain().start(CommandLineOptions.from(args));
            if (state.status() != PipelineStatus.FAILED) {
                LOGGER.info(completionMessage(state));
                System.exit(0);
            } else {
                LOGGER.error(completionMessage(state));
                System.exit(1);
            }
        } catch (ParseException e) {
            LOGGER.error("Exiting due to incorrect arguments");
            System.exit(1);
        } catch (Exception e) {
            LOGGER.error("An unexpected issue arose while running the pipeline. See the attached exception for more details.", e);
            System.exit(1);
        }
    }

    public static String completionMessage(final PipelineState state) {
        return String.format("Pipeline completed with status [%s], summary: [%s]", state.status(), state);
    }

    public PipelineState start(final Arguments arguments) {
        LOGGER.info("Arguments are [{}]", arguments);
        VersionUtils.printAll();
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            Storage storage = StorageProvider.from(arguments, credentials).get();
            PipelineInput input = JsonPipelineInput.read(arguments.sampleJson());
            final OutputPublisher outputPublisher = getOutputPublisher(arguments, storage);
            MetadataProvider metadataProvider = new MetadataProvider(arguments, input);
            SingleSampleEventListener referenceEventListener = new SingleSampleEventListener();
            SingleSampleEventListener tumorEventListener = new SingleSampleEventListener();
            SomaticRunMetadata somaticRunMetadata = metadataProvider.get();
            InputMode mode = new ModeResolver().apply(somaticRunMetadata);
            RunApi runApi = HmfApi.create(arguments.hmfApiUrl().orElse("")).runs();
            HmfApiStatusUpdate apiStatusUpdateOrNot = HmfApiStatusUpdate.from(arguments, runApi, input);
            try (var turquoise = Turquoise.create(PublisherProvider.from(arguments, credentials).get("turquoise.events"), arguments, somaticRunMetadata)) {
                LOGGER.info("Starting pipeline in [{}] mode", mode);
                apiStatusUpdateOrNot.start();
                turquoise.publishStarted();
                BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue = new ArrayBlockingQueue<>(1);
                BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue = new ArrayBlockingQueue<>(1);
                BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue = new ArrayBlockingQueue<>(1);
                BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue = new ArrayBlockingQueue<>(1);
                BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue = new ArrayBlockingQueue<>(1);
                StartingPoint startingPoint = new StartingPoint(arguments);
                PersistedDataset persistedDataset = new InputPersistedDataset(input, arguments.project());
                PipelineState state = new FullPipeline(singleSamplePipeline(arguments,
                        input,
                        credentials,
                        storage,
                        referenceEventListener,
                        somaticRunMetadata,
                        referenceBamMetricsOutputQueue,
                        germlineCallerOutputQueue,
                        referenceFlagstatOutputQueue,
                        startingPoint,
                        persistedDataset,
                        mode),
                        singleSamplePipeline(arguments,
                                input,
                                credentials,
                                storage,
                                tumorEventListener,
                                somaticRunMetadata,
                                tumorBamMetricsOutputQueue,
                                germlineCallerOutputQueue,
                                tumorFlagstatOutputQueue,
                                startingPoint,
                                persistedDataset,
                                mode),
                        somaticPipeline(arguments,
                                credentials,
                                storage,
                                somaticRunMetadata,
                                referenceBamMetricsOutputQueue,
                                tumorBamMetricsOutputQueue,
                                referenceFlagstatOutputQueue,
                                tumorFlagstatOutputQueue,
                                startingPoint,
                                persistedDataset,
                                mode),
                        metadataProvider.get(),
                        Executors.newCachedThreadPool(),
                        referenceEventListener,
                        tumorEventListener,
                        CleanupProvider.from(arguments, storage).get(),
                        outputPublisher,
                        apiStatusUpdateOrNot).run();
                VmExecutionLogSummary.ofFailedStages(storage, state);
                turquoise.publishComplete(state.status().toString());
                return state;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private OutputPublisher getOutputPublisher(final Arguments arguments, final Storage storage) throws IOException {
        if (arguments.context().equals(Pipeline.Context.PLATINUM)) {
            return new NoopOutputPublisher();
        } else {
            var sourceBucket = storage.get(arguments.outputBucket());
            var eventPublisher = createPubsubEventPublisher(arguments);
            var pipelineContext = arguments.context();
            return new PipelineCompleteEventPublisher(sourceBucket, eventPublisher, pipelineContext, arguments.outputCram());
        }
    }

    private static EventPublisher<PipelineComplete> createPubsubEventPublisher(Arguments arguments) throws IOException {
        var pubsubProject = arguments.pubsubProject().orElse(arguments.project());
        var eventContext = EventContext.builder()
                .environment(arguments.pubsubTopicEnvironment().orElseThrow(PipelineMain::missingPubsubArgumentsException))
                .workflow(arguments.pubsubTopicEnvironment().orElseThrow(PipelineMain::missingPubsubArgumentsException))
                .build();
        return new PubsubEventBuilder().newPublisher(pubsubProject, eventContext, new PipelineComplete.EventDescriptor());
    }
}
