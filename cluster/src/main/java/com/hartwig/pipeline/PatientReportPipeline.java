package com.hartwig.pipeline;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerProvider;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCallerProvider;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerProvider;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.storage.StorageProvider;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.amber.AmberProvider;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltProvider;

import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientReportPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatientReportPipeline.class);

    private final Aligner aligner;
    private final GermlineCaller germlineCaller;
    private final SomaticCaller somaticCaller;
    private final StructuralCaller structuralCaller;
    private final Amber amber;
    private final Cobalt cobalt;
    private final AlignmentOutputStorage alignmentOutputStorage;
    private final Arguments arguments;
    private final ExecutorService executorService;

    private PatientReportPipeline(final Aligner aligner, final GermlineCaller germlineCaller, final SomaticCaller somaticCaller,
            final StructuralCaller structuralCaller, final Amber amber, final Cobalt cobalt,
            final AlignmentOutputStorage alignmentOutputStorage, final Arguments arguments, final ExecutorService executorService) {
        this.aligner = aligner;
        this.germlineCaller = germlineCaller;
        this.somaticCaller = somaticCaller;
        this.structuralCaller = structuralCaller;
        this.amber = amber;
        this.cobalt = cobalt;
        this.alignmentOutputStorage = alignmentOutputStorage;
        this.arguments = arguments;
        this.executorService = executorService;
    }

    public void run() throws Exception {
        AlignmentOutput alignmentOutput = arguments.runAligner()
                ? aligner.run()
                : alignmentOutputStorage.get(Sample.builder(arguments.sampleId()).build())
                        .orElseThrow(() -> new IllegalArgumentException("Unable to find output for sample [%s]. "
                                + "Please run the aligner first by setting -run_aligner to true"));

        Optional<Future<GermlineCallerOutput>> maybeGermlineCallerFuture = Optional.empty();
        Optional<Future<SomaticCallerOutput>> maybeSomaticCallerFuture = Optional.empty();
        Optional<Future<StructuralCallerOutput>> maybeStructuralCallerFuture = Optional.empty();
        Optional<Future<AmberOutput>> maybeAmberOutputFuture = Optional.empty();
        Optional<Future<CobaltOutput>> maybeCobaltOutputFuture = Optional.empty();

        if (arguments.runGermlineCaller()) {
            maybeGermlineCallerFuture = Optional.of(executorService.submit(() -> germlineCaller.run(alignmentOutput)));
        }

        if (arguments.runStructuralCaller() || arguments.runSomaticCaller() || arguments.runTertiary()) {
            Optional<AlignmentPair> maybeAlignmentPair = alignmentOutputStorage.get(mate(alignmentOutput.sample()))
                    .map(complement -> AlignmentPair.of(alignmentOutput, complement));

            if (arguments.runSomaticCaller()) {
                maybeSomaticCallerFuture = maybeAlignmentPair.map(pair -> executorService.submit(() -> somaticCaller.run(pair)));
            }
            if (arguments.runStructuralCaller()) {
                maybeStructuralCallerFuture = maybeAlignmentPair.map(pair -> executorService.submit(() -> structuralCaller.run(pair)));
            }
            if (arguments.runTertiary()) {
                maybeAmberOutputFuture = maybeAlignmentPair.map(pair -> executorService.submit(() -> amber.run(pair)));
                maybeCobaltOutputFuture = maybeAlignmentPair.map(pair -> executorService.submit(() -> cobalt.run(pair)));
            }
        }

        Optional<GermlineCallerOutput> germlineCallerOutput = maybeGermlineCallerFuture.map(PatientReportPipeline::futurePayload);
        Optional<SomaticCallerOutput> somaticCallerOutput = maybeSomaticCallerFuture.map(PatientReportPipeline::futurePayload);
        Optional<StructuralCallerOutput> structuralCallerOutput = maybeStructuralCallerFuture.map(PatientReportPipeline::futurePayload);
        Optional<AmberOutput> amberOutput = maybeAmberOutputFuture.map(PatientReportPipeline::futurePayload);
        Optional<CobaltOutput> cobaltOutput = maybeCobaltOutputFuture.map(PatientReportPipeline::futurePayload);

        germlineCallerOutput.ifPresent(output -> checkStatus("Germline", output.status()));
        somaticCallerOutput.ifPresent(output -> checkStatus("Somatic", output.status()));
        structuralCallerOutput.ifPresent(output -> checkStatus("Structural", output.status()));
        amberOutput.ifPresent(output -> checkStatus("Amber", output.status()));
        cobaltOutput.ifPresent(output -> checkStatus("Cobalt", output.status()));

    }

    private static <T> T futurePayload(final Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkStatus(final String callerName, final JobStatus status) {
        if (status == JobStatus.FAILED) {
            LOGGER.error("[{}] caller failed on the remote VM, no reason available here. Check the run.log in the output bucket",
                    callerName);
        }
    }

    public static void main(String[] args) {
        try {
            Arguments arguments = CommandLineOptions.from(args);
            LOGGER.info("Arguments [{}]", arguments);
            try {
                GoogleCredentials credentials = CredentialProvider.from(arguments).get();
                Storage storage = StorageProvider.from(arguments, credentials).get();
                new PatientReportPipeline(AlignerProvider.from(credentials, storage, arguments).get(),
                        GermlineCallerProvider.from(credentials, storage, arguments).get(),
                        SomaticCallerProvider.from(arguments, credentials, storage).get(),
                        StructuralCallerProvider.from(arguments).get(),
                        AmberProvider.from(arguments, credentials, storage).get(),
                        CobaltProvider.from(arguments, credentials, storage).get(),
                        new AlignmentOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory()),
                        arguments,
                        Executors.newFixedThreadPool(4)).run();
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the pipeline. See the attached exception for more details.", e);
                System.exit(1);
            }
            LOGGER.info("Patient report pipeline completed successfully");
        } catch (ParseException e) {
            LOGGER.info("Exiting due to incorrect arguments");
        }
    }

    private static Sample mate(Sample sample) {
        if (sample.type().equals(Sample.Type.REFERENCE)) {
            return replaceSuffix(sample, "T").type(Sample.Type.TUMOR).build();
        } else {
            return replaceSuffix(sample, "R").type(Sample.Type.REFERENCE).build();
        }
    }

    @NotNull
    private static ImmutableSample.Builder replaceSuffix(final Sample sample, final String newSuffix) {
        return Sample.builder(sample.directory(), sample.name().substring(0, sample.name().length() - 1).concat(newSuffix));
    }
}
