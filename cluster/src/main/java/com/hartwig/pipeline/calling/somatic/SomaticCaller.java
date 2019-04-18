package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.execution.vm.OutputUpload.OUTPUT_DIRECTORY;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.JobComplete;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceLocation;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticCaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticCaller.class);
    private static final String STRELKA_ANALYSIS_DIRECTORY = "/strelkaAnalysis";
    private static final int STRELKA_THREADS = 8;

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final Resource referenceGenome;
    private final Resource strelkaConfig;
    private final ResultsDirectory resultsDirectory;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final Resource referenceGenome,
            final Resource strelkaConfig, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.referenceGenome = referenceGenome;
        this.strelkaConfig = strelkaConfig;
        this.resultsDirectory = resultsDirectory;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        LOGGER.info("Starting Somatic Calling");
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);
        ResourceLocation referenceGenomeLocation = referenceGenome.copyInto(runtimeBucket);
        ResourceLocation strelkaConfigLocation = strelkaConfig.copyInto(runtimeBucket);

        InputDownload downloadReferenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload downloadReferenceBai = new InputDownload(pair.reference().finalBaiLocation());
        InputDownload downloadTumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload downloadTumorBai = new InputDownload(pair.tumor().finalBaiLocation());

        ResourceDownload referenceGenomeDownload = new ResourceDownload(referenceGenomeLocation, runtimeBucket);
        String referenceGenomeFile = localReferenceGenomeFile(referenceGenomeLocation, referenceGenomeDownload);

        ResourceDownload configDownload = new ResourceDownload(strelkaConfigLocation, runtimeBucket);
        String strelkaConfigFile = localStrelkaConfigFile(strelkaConfigLocation, configDownload);

        String strelkaAnalysisOutput = OUTPUT_DIRECTORY + STRELKA_ANALYSIS_DIRECTORY;
        BashStartupScript strelkaBash = BashStartupScript.of(OUTPUT_DIRECTORY, OUTPUT_DIRECTORY + "/strelka.log")
                .addCommand(downloadReferenceBam)
                .addCommand(downloadReferenceBai)
                .addCommand(downloadTumorBam)
                .addCommand(downloadTumorBai)
                .addCommand(referenceGenomeDownload)
                .addCommand(configDownload)
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
                .addCommand(new JobComplete(BashStartupScript.JOB_COMPLETE))
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

    @NotNull
    private String localStrelkaConfigFile(final ResourceLocation strelkaConfigLocation, final ResourceDownload strelkaConfigDownload) {
        return strelkaConfigDownload.getLocalPaths()
                .stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No strelka configuration file was found in resource [%s]",
                        strelkaConfigLocation)));
    }

    @NotNull
    private String localReferenceGenomeFile(final ResourceLocation referenceGenomeLocation,
            final ResourceDownload referenceGenomeDownload) {
        return referenceGenomeDownload.getLocalPaths()
                .stream()
                .filter(file -> file.endsWith("fa") || file.endsWith("fasta"))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("No reference genome fasta file was found in resource [%s]",
                        referenceGenomeLocation)));
    }
}
