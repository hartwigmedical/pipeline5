package com.hartwig.pipeline;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsProvider;
import com.hartwig.pipeline.calling.germline.GermlineCallerProvider;
import com.hartwig.pipeline.calling.somatic.SomaticCallerProvider;
import com.hartwig.pipeline.calling.structural.StructuralCallerProvider;
import com.hartwig.pipeline.cleanup.CleanupProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.flagstat.FlagstatProvider;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.metadata.SampleMetadataApiProvider;
import com.hartwig.pipeline.metadata.SetMetadataApiProvider;
import com.hartwig.pipeline.report.PatientReportProvider;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tertiary.amber.AmberProvider;
import com.hartwig.pipeline.tertiary.cobalt.CobaltProvider;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckerProvider;
import com.hartwig.pipeline.tertiary.purple.PurpleProvider;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineMain.class);

    public static void main(String[] args) {
        try {
            Arguments arguments = CommandLineOptions.from(args);
            LOGGER.info("Arguments [{}]", arguments);
            try {
                GoogleCredentials credentials = CredentialProvider.from(arguments).get();
                Storage storage = StorageProvider.from(arguments, credentials).get();
                if (arguments.mode().equals(Arguments.Mode.SINGLE_SAMPLE)) {
                    PipelineState state = new SingleSamplePipeline(SampleMetadataApiProvider.from(arguments).get(),
                            AlignerProvider.from(credentials, storage, arguments).get(),
                            BamMetricsProvider.from(arguments, credentials, storage).get(),
                            GermlineCallerProvider.from(credentials, storage, arguments).get(),
                            new SnpGenotype(arguments,
                                    ComputeEngine.from(arguments, credentials),
                                    storage,
                                    ResultsDirectory.defaultDirectory()),
                            FlagstatProvider.from(arguments, credentials, storage).get(),
                            PatientReportProvider.from(storage, arguments).get(),
                            Executors.newCachedThreadPool(),
                            arguments).run();
                    LOGGER.info("Single sample pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
                } else {
                    PipelineState state =
                            new SomaticPipeline(new AlignmentOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                                    new BamMetricsOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                                    SetMetadataApiProvider.from(arguments).get(),
                                    PatientReportProvider.from(storage, arguments).get(),
                                    CleanupProvider.from(credentials, arguments, storage).get(),
                                    AmberProvider.from(arguments, credentials, storage).get(),
                                    CobaltProvider.from(arguments, credentials, storage).get(),
                                    SomaticCallerProvider.from(arguments, credentials, storage).get(),
                                    StructuralCallerProvider.from(arguments, credentials, storage).get(),
                                    PurpleProvider.from(arguments, credentials, storage).get(),
                                    HealthCheckerProvider.from(arguments, credentials, storage).get(),
                                    Executors.newCachedThreadPool()).run();
                    LOGGER.info("Somatic pipeline is complete with status [{}]. Stages run were [{}]", state.status(), state);
                }
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the pipeline. See the attached exception for more details.", e);
                System.exit(1);
            }
            System.exit(0);
        } catch (ParseException e) {
            LOGGER.info("Exiting due to incorrect arguments");
        }
    }
}
