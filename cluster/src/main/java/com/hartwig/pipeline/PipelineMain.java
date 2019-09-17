package com.hartwig.pipeline;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.calling.germline.GermlineCallerProvider;
import com.hartwig.pipeline.calling.somatic.SomaticCallerProvider;
import com.hartwig.pipeline.calling.structural.StructuralCallerProvider;
import com.hartwig.pipeline.cleanup.CleanupProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.flagstat.FlagstatProvider;
import com.hartwig.pipeline.metadata.LocalSampleMetadataApi;
import com.hartwig.pipeline.metadata.SampleMetadataApi;
import com.hartwig.pipeline.metadata.SampleMetadataApiProvider;
import com.hartwig.pipeline.metadata.SetMetadataApiProvider;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.metrics.BamMetricsProvider;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResultsProvider;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tertiary.amber.AmberProvider;
import com.hartwig.pipeline.tertiary.cobalt.CobaltProvider;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckerProvider;
import com.hartwig.pipeline.tertiary.purple.PurpleProvider;
import com.hartwig.pipeline.tools.Versions;

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
            PipelineState state;
            if (arguments.mode().equals(Arguments.Mode.FULL)) {
                String referenceSample = arguments.setId() + "R";
                String tumorSample = arguments.setId() + "T";
                LocalSampleMetadataApi referenceApi = new LocalSampleMetadataApi(referenceSample);
                LocalSampleMetadataApi tumorApi = new LocalSampleMetadataApi(tumorSample);
                state = new FullPipeline(singleSamplePipeline(addSampleId(arguments, referenceSample), credentials, storage, referenceApi),
                        singleSamplePipeline(addSampleId(arguments, tumorSample), credentials, storage, tumorApi),
                        somaticPipeline(arguments, credentials, storage),
                        Executors.newCachedThreadPool(),
                        referenceApi,
                        tumorApi).run();
            } else if (arguments.mode().equals(Arguments.Mode.SINGLE_SAMPLE)) {
                state = singleSamplePipeline(arguments, credentials, storage, SampleMetadataApiProvider.from(arguments).get()).run();
                LOGGER.info("Single sample pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
            } else {
                state = somaticPipeline(arguments, credentials, storage).run();
                LOGGER.info("Somatic pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
            }
            return state;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SomaticPipeline somaticPipeline(final Arguments arguments, final GoogleCredentials credentials, final Storage storage)
            throws Exception {
        SomaticMetadataApi somaticMetadataApi = SetMetadataApiProvider.from(arguments, storage).get();
        return new SomaticPipeline(new AlignmentOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                new BamMetricsOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                somaticMetadataApi,
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                new FullSomaticResults(storage, arguments),
                CleanupProvider.from(credentials, arguments, storage, somaticMetadataApi).get(),
                AmberProvider.from(arguments, credentials, storage).get(),
                CobaltProvider.from(arguments, credentials, storage).get(),
                SomaticCallerProvider.from(arguments, credentials, storage).get(),
                StructuralCallerProvider.from(arguments, credentials, storage).get(),
                PurpleProvider.from(arguments, credentials, storage).get(),
                HealthCheckerProvider.from(arguments, credentials, storage).get(),
                Executors.newCachedThreadPool());
    }

    private static Arguments addSampleId(final Arguments arguments, final String referenceSample) {
        return Arguments.builder().from(arguments).sampleId(referenceSample).build();
    }

    private static SingleSamplePipeline singleSamplePipeline(final Arguments arguments, final GoogleCredentials credentials,
            final Storage storage, final SampleMetadataApi sampleMetadataApi) throws Exception {
        return new SingleSamplePipeline(sampleMetadataApi,
                AlignerProvider.from(credentials, storage, arguments).get(),
                BamMetricsProvider.from(arguments, credentials, storage).get(),
                GermlineCallerProvider.from(credentials, storage, arguments).get(),
                new SnpGenotype(arguments, ComputeEngine.from(arguments, credentials), storage, ResultsDirectory.defaultDirectory()),
                FlagstatProvider.from(arguments, credentials, storage).get(),
                PipelineResultsProvider.from(storage, arguments, Versions.pipelineVersion()).get(),
                Executors.newCachedThreadPool(),
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
