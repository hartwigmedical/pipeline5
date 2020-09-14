package com.hartwig.pipeline;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.cleanup.CleanupProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticMetadataApiProvider;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.pubsub.PublisherProvider;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.startingpoint.StartingPoint;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tools.Versions;
import com.hartwig.pipeline.turquoise.PipelineCompleted;
import com.hartwig.pipeline.turquoise.PipelineProperties;
import com.hartwig.pipeline.turquoise.PipelineStarted;
import com.hartwig.pipeline.turquoise.TurquoiseEvent;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineMain.class);

    public PipelineState start(Arguments arguments) {
        LOGGER.info("Arguments [{}]", arguments);
        Versions.printAll();
        try {
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            Storage storage = StorageProvider.from(arguments, credentials).get();
            Publisher publisher = PublisherProvider.from(arguments, credentials).get();
            SomaticMetadataApi somaticMetadataApi = SomaticMetadataApiProvider.from(arguments, storage).get();
            SingleSampleEventListener referenceEventListener = new SingleSampleEventListener();
            SingleSampleEventListener tumorEventListener = new SingleSampleEventListener();
            SomaticRunMetadata somaticRunMetadata = somaticMetadataApi.get();
            boolean isSingleSample = somaticRunMetadata.isSingleSample();
            String ini = somaticRunMetadata.isSingleSample() ? "single_sample" : arguments.shallow() ? "shallow" : "somatic";
            PipelineProperties eventSubjects = PipelineProperties.builder()
                    .sample(somaticRunMetadata.maybeTumor()
                            .map(SingleSampleRunMetadata::sampleName)
                            .orElse(somaticRunMetadata.reference().sampleName()))
                    .runId(arguments.sbpApiRunId())
                    .set(somaticRunMetadata.runName())
                    .referenceBarcode(somaticRunMetadata.reference().sampleId())
                    .tumorBarcode(somaticRunMetadata.maybeTumor().map(SingleSampleRunMetadata::sampleId))
                    .type(ini)
                    .build();
            startedEvent(eventSubjects, publisher, arguments.publishToTurquoise());
            BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue = new ArrayBlockingQueue<>(1);
            BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue = new ArrayBlockingQueue<>(1);
            BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue = new ArrayBlockingQueue<>(1);
            StartingPoint startingPoint = new StartingPoint(arguments);
            PipelineState state = new FullPipeline(singleSamplePipeline(arguments,
                    credentials,
                    storage,
                    referenceEventListener,
                    isSingleSample,
                    somaticRunMetadata.runName(),
                    referenceBamMetricsOutputQueue,
                    germlineCallerOutputQueue,
                    startingPoint),
                    singleSamplePipeline(arguments,
                            credentials,
                            storage,
                            tumorEventListener,
                            isSingleSample,
                            somaticRunMetadata.runName(),
                            tumorBamMetricsOutputQueue,
                            germlineCallerOutputQueue,
                            startingPoint),
                    somaticPipeline(arguments,
                            credentials,
                            storage,
                            somaticMetadataApi,
                            referenceBamMetricsOutputQueue,
                            tumorBamMetricsOutputQueue,
                            germlineCallerOutputQueue,
                            startingPoint,
                            somaticRunMetadata.runName()),
                    Executors.newCachedThreadPool(),
                    referenceEventListener,
                    tumorEventListener,
                    somaticMetadataApi,
                    new FullSomaticResults(storage, arguments),
                    CleanupProvider.from(arguments, storage).get()).run();
            completedEvent(eventSubjects, publisher, state.status().toString(), arguments.publishToTurquoise());
            return state;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publish(final TurquoiseEvent turquoiseEvent, final boolean publish) {
        if (publish) {
            turquoiseEvent.publish();
        }
    }

    public void completedEvent(final PipelineProperties properties, final Publisher publisher, final String status, boolean publish) {
        publish(PipelineCompleted.builder().properties(properties).publisher(publisher).status(status).build(), publish);
    }

    public void startedEvent(final PipelineProperties subjects, final Publisher publisher, boolean publish) {
        publish(PipelineStarted.builder().properties(subjects).publisher(publisher).build(), publish);
    }

    private static SomaticPipeline somaticPipeline(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SomaticMetadataApi somaticMetadataApi, final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumourBamMetricsOutputQueue,
            final BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue, final StartingPoint startingPoint, final String runName)
            throws Exception {
        return new SomaticPipeline(arguments,
                new StageRunner<>(storage,
                        arguments,
                        GoogleComputeEngine.from(arguments, credentials),
                        ResultsDirectory.defaultDirectory(),
                        startingPoint,
                        runName),
                referenceBamMetricsOutputQueue,
                tumourBamMetricsOutputQueue,
                germlineCallerOutputQueue,
                somaticMetadataApi,
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                Executors.newCachedThreadPool());
    }

    private static SingleSamplePipeline singleSamplePipeline(final Arguments arguments, final GoogleCredentials credentials,
            final Storage storage, final SingleSampleEventListener eventListener, final Boolean isStandalone, final String runName,
            final BlockingQueue<BamMetricsOutput> metricsOutputQueue, final BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue,
            final StartingPoint startingPoint) throws Exception {

        return new SingleSamplePipeline(eventListener,
                new StageRunner<>(storage,
                        arguments,
                        GoogleComputeEngine.from(arguments, credentials),
                        ResultsDirectory.defaultDirectory(),
                        startingPoint,
                        runName),
                AlignerProvider.from(credentials, storage, runName, arguments).get(),
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                Executors.newCachedThreadPool(),
                isStandalone,
                arguments,
                metricsOutputQueue,
                germlineCallerOutputQueue,
                runName);
    }

    public static void main(String[] args) {
        try {
            PipelineState state = new PipelineMain().start(CommandLineOptions.from(args));
            if (state.status() != PipelineStatus.FAILED) {
                System.exit(0);
            } else {
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
}
