package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceLocation;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hartwig.pipeline.execution.vm.OutputUpload.OUTPUT_DIRECTORY;

public class SomaticCaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticCaller.class);
    private static final String STRELKA_ANALYSIS_DIRECTORY = "/strelkaAnalysis";
    private static final int STRELKA_THREADS = 8;

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final Resource referenceGenome;
    private final Resource strelkaConfig;
    private final Resource mappability;
    private final ResultsDirectory resultsDirectory;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final Resource referenceGenome,
            final Resource strelkaConfig, final Resource mappability, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.referenceGenome = referenceGenome;
        this.strelkaConfig = strelkaConfig;
        this.mappability = mappability;
        this.resultsDirectory = resultsDirectory;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        LOGGER.info("Starting Somatic Calling");
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);

        ResourceLocation referenceGenomeLocation = referenceGenome.copyInto(runtimeBucket);
        ResourceLocation strelkaConfigLocation = strelkaConfig.copyInto(runtimeBucket);
        ResourceLocation mappabilityLocation = mappability.copyInto(runtimeBucket);

        InputDownload downloadReferenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload downloadReferenceBai = new InputDownload(pair.reference().finalBaiLocation());
        InputDownload downloadTumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload downloadTumorBai = new InputDownload(pair.tumor().finalBaiLocation());

        ResourceDownload referenceGenomeDownload = new ResourceDownload(referenceGenomeLocation, runtimeBucket);
        String referenceGenomeFile = referenceGenomeDownload.find("fa", "fasta");

        ResourceDownload configDownload = new ResourceDownload(strelkaConfigLocation, runtimeBucket);
        String strelkaConfigFile = configDownload.find("ini");

        ResourceDownload mappabilityDownload = new ResourceDownload(mappabilityLocation, runtimeBucket);
        String mappabilityBed = mappabilityDownload.find("bed.gz");
        String mappabilityHdr = mappabilityDownload.find("hdr");
        String strelkaAnalysisOutput = OUTPUT_DIRECTORY + STRELKA_ANALYSIS_DIRECTORY;
        String mappabilityAnnotatedVcf = "/data/output/mappability.annotated.vcf";
        BashStartupScript strelkaBash = BashStartupScript.of(OUTPUT_DIRECTORY)
                .addCommand(downloadReferenceBam)
                .addCommand(downloadReferenceBai)
                .addCommand(downloadTumorBam)
                .addCommand(downloadTumorBai)
                .addCommand(referenceGenomeDownload)
                .addCommand(configDownload)
                .addCommand(mappabilityDownload)
                .addCommand(new ConfigureStrelkaWorkflowCommand(downloadTumorBam.getLocalTargetPath(),
                        downloadReferenceBam.getLocalTargetPath(),
                        strelkaConfigFile,
                        referenceGenomeFile,
                        strelkaAnalysisOutput))
                .addCommand(new MakeStrelka(strelkaAnalysisOutput, STRELKA_THREADS))
                .addCommand(new CombineVcfsCommand(referenceGenomeFile,
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        OUTPUT_DIRECTORY))
                .addCommand(new AnnotateMappabilityCommand(mappabilityBed,
                        mappabilityHdr,
                        "/data/output/combined.vcf",
                        mappabilityAnnotatedVcf))
                .addCommand(new TabixCommand(mappabilityAnnotatedVcf))
                .addCommand(new JobComplete(BashStartupScript.COMPLETION_FLAG_FILENAME))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));

        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(strelkaBash));

        return SomaticCallerOutput.builder()
                .allSomaticIndelsVcf(vcfLocation(runtimeBucket, "all.somatic.indels.vcf"))
                .allSomaticSnvsVcf(vcfLocation(runtimeBucket, "all.somatic.snvs.vcf"))
                .passedSomaticIndelsVcf(vcfLocation(runtimeBucket, "passed.somatic.indels.vcf"))
                .passedSomaticSnvsVcf(vcfLocation(runtimeBucket, "passed.somatic.snvs.vcf"))
                .build();
    }

    @NotNull
    private GoogleStorageLocation vcfLocation(final RuntimeBucket runtimeBucket, final String vcfName) {
        return GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(STRELKA_ANALYSIS_DIRECTORY + "/results/" + vcfName));
    }
}
