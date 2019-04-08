package com.hartwig.pipeline;

import java.util.Optional;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.input.Samples;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignerProvider;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerProvider;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCallerProvider;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerProvider;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.storage.StorageProvider;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatientReportPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatientReportPipeline.class);

    private final Aligner aligner;
    private final GermlineCaller germlineCaller;
    private final SomaticCaller somaticCaller;
    private final StructuralCaller structuralCaller;
    private final AlignmentOutputStorage alignmentOutputStorage;

    private PatientReportPipeline(final Aligner aligner, final GermlineCaller germlineCaller, final SomaticCaller somaticCaller,
            final StructuralCaller structuralCaller, final AlignmentOutputStorage alignmentOutputStorage) {
        this.aligner = aligner;
        this.germlineCaller = germlineCaller;
        this.somaticCaller = somaticCaller;
        this.structuralCaller = structuralCaller;
        this.alignmentOutputStorage = alignmentOutputStorage;
    }

    public void run() throws Exception {
        AlignmentOutput alignmentOutput = aligner.run();
        GermlineCallerOutput germlineCallerOutput = germlineCaller.run(alignmentOutput);

        Optional<AlignmentPair> maybeAlignmentPair = alignmentOutputStorage.get(Samples.complement(alignmentOutput.sample()))
                .map(complement -> AlignmentPair.of(alignmentOutput, complement));

        Optional<SomaticCallerOutput> maybeSomaticCallerOutput = maybeAlignmentPair.map(somaticCaller::run);
        Optional<StructuralCallerOutput> maybeStructuralCallerOutput = maybeAlignmentPair.map(structuralCaller::run);
    }

    public static void main(String[] args) {
        try {
            Arguments arguments = CommandLineOptions.from(args);
            LOGGER.info("Arguments [{}]", arguments);
            try {
                GoogleCredentials credentials = CredentialProvider.from(arguments).get();
                Storage storage = StorageProvider.from(arguments, credentials).get();
                new PatientReportPipeline(AlignerProvider.from(credentials, storage, arguments).get(),
                        GermlineCallerProvider.from(arguments).get(),
                        SomaticCallerProvider.from(arguments).get(),
                        StructuralCallerProvider.from(arguments).get(),
                        new AlignmentOutputStorage(storage, arguments, ResultsDirectory.defaultDirectory())).run();
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the pipeline. See the attached exception for more details.", e);
                System.exit(1);
            }
            LOGGER.info("Patient report pipeline completed successfully");
        } catch (ParseException e) {
            LOGGER.info("Exiting due to incorrect arguments");
        }
    }
}
