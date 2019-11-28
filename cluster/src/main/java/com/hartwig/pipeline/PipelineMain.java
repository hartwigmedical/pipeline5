package com.hartwig.pipeline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.cleanup.CleanupProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.metadata.*;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tools.Versions;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class PipelineMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineMain.class);

    public PipelineState start(Arguments arguments) {
        LOGGER.info("Arguments [{}]", arguments);
        Versions.printAll();
        try {
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            Storage storage = StorageProvider.from(arguments, credentials).get();
            SomaticMetadataApi somaticMetadataApi = SetMetadataApiProvider.from(arguments, storage).get();
            PipelineState state;
            if (arguments.mode().equals(Arguments.Mode.FULL)) {
                SingleSampleEventListener referenceEventListener = new SingleSampleEventListener();
                SingleSampleEventListener tumorEventListener = new SingleSampleEventListener();
                state = new FullPipeline(singleSamplePipeline(arguments, credentials, storage, referenceEventListener, false),
                        singleSamplePipeline(arguments, credentials, storage, tumorEventListener, false),
                        somaticPipeline(arguments, credentials, storage, somaticMetadataApi),
                        Executors.newCachedThreadPool(),
                        referenceEventListener,
                        tumorEventListener,
                        somaticMetadataApi.get()).run();
            } else if (arguments.mode().equals(Arguments.Mode.SINGLE_SAMPLE)) {
                SampleMetadataApi referenceApi = SampleMetadataApiProvider.from(arguments).get();
                state = singleSamplePipeline(arguments,
                        credentials,
                        storage,
                        new SingleSampleEventListener(),
                        true).run(referenceApi.get());
                LOGGER.info("Single sample pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
            } else {
                state = somaticPipeline(arguments, credentials, storage, somaticMetadataApi).run();
                LOGGER.info("Somatic pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
            }
            return state;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SomaticPipeline somaticPipeline(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SomaticMetadataApi somaticMetadataApi) throws Exception {
        return new SomaticPipeline(arguments,
                new StageRunner<>(storage,
                        arguments,
                        ComputeEngine.from(arguments, credentials),
                        ResultsDirectory.defaultDirectory()),
                new AlignmentOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                new OutputStorage<>(ResultsDirectory.defaultDirectory(),
                        arguments,
                        metadata -> RuntimeBucket.from(storage, BamMetrics.NAMESPACE, metadata, arguments)),
                new OutputStorage<>(ResultsDirectory.defaultDirectory(),
                        arguments,
                        metadata -> RuntimeBucket.from(storage, GermlineCaller.NAMESPACE, metadata, arguments)), somaticMetadataApi,
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                new FullSomaticResults(storage, arguments),
                CleanupProvider.from(arguments, storage).get(),
                Executors.newCachedThreadPool());
    }

    private static SingleSamplePipeline singleSamplePipeline(final Arguments arguments, final GoogleCredentials credentials,
            final Storage storage, final SingleSampleEventListener eventListener, final Boolean isStandalone) throws Exception {
        return new SingleSamplePipeline(eventListener,
                new StageRunner<>(storage,
                        arguments,
                        ComputeEngine.from(arguments, credentials),
                        ResultsDirectory.defaultDirectory()),
                AlignerProvider.from(credentials, storage, arguments).get(),
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                Executors.newCachedThreadPool(),
                isStandalone,
                arguments);
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
        } catch (Exception e) {
            LOGGER.error("An unexpected issue arose while running the pipeline. See the attached exception for more details.", e);
            System.exit(1);
        }
    }
}
