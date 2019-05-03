package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.NamespacedResults;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

import org.jetbrains.annotations.NotNull;

public class SomaticCaller {

    public static final String RESULTS_NAMESPACE = "somatic_caller";
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final NamespacedResults namespacedResults;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final NamespacedResults namespacedResults) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.namespacedResults = namespacedResults;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {

        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload = ResourceDownload.from(runtimeBucket, referenceGenomeResource());
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");
        ResourceDownload strelkaConfigDownload = ResourceDownload.from(storage, "strelka_config", runtimeBucket);
        ResourceDownload mappabilityResources = ResourceDownload.from(storage, "hg19_mappability_tracks", runtimeBucket);
        ResourceDownload ponv2Resources = ResourceDownload.from(storage, "pon-v2", runtimeBucket);
        ResourceDownload strelkaPostProcessBedResource = ResourceDownload.from(storage, "beds", runtimeBucket);
        ResourceDownload sageResourceDownload = ResourceDownload.from(storage, "sage-pilot", runtimeBucket);
        ResourceDownload snpEffResourceDownload = ResourceDownload.from(storage, "snpeff", runtimeBucket);
        ResourceDownload knownSnpsResourceDownload = ResourceDownload.from(storage, "known_snps", runtimeBucket);
        ResourceDownload cosmicResourceDownload = ResourceDownload.from(storage, "cosmic_v85", runtimeBucket);
        bash.addCommand(referenceGenomeDownload)
                .addCommand(strelkaConfigDownload)
                .addCommand(mappabilityResources)
                .addCommand(ponv2Resources)
                .addCommand(strelkaPostProcessBedResource)
                .addCommand(sageResourceDownload)
                .addCommand(snpEffResourceDownload)
                .addCommand(knownSnpsResourceDownload)
                .addCommand(cosmicResourceDownload);

        InputDownload tumorBam = new InputDownload(pair.tumor().recalibratedBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().recalibratedBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().recalibratedBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().recalibratedBaiLocation());
        bash.addCommand(tumorBam).addCommand(referenceBam).addCommand(tumorBai).addCommand(referenceBai);

        String tumorBamPath = tumorBam.getLocalTargetPath();
        String referenceBamPath = referenceBam.getLocalTargetPath();
        SubStageInputOutput sageOutput = new SageHotspotsApplication(sageResourceDownload.find("tsv"),
                sageResourceDownload.find("bed"),
                referenceGenomePath,
                tumorBamPath,
                referenceBamPath,
                tumorSampleName,
                referenceSampleName).andThen(new SageFiltersAndAnnotations(tumorSampleName))
                .andThen(new SagePonAnnotation(sageResourceDownload.find("SAGE_PON.vcf.gz")))
                .andThen(new SagePonFilter())
                .apply(SubStageInputOutput.of(tumorSampleName, OutputFile.empty(), bash));

        SubStageInputOutput mergedOutput = new Strelka(referenceBamPath,
                tumorBamPath,
                strelkaConfigDownload.find("ini"),
                referenceGenomePath).andThen(new MappabilityAnnotation(mappabilityResources.find("bed.gz"),
                mappabilityResources.find("hdr")))
                .andThen(new PonAnnotation("germline.pon", ponv2Resources.find("GERMLINE_PON.vcf.gz"), "GERMLINE_PON_COUNT"))
                .andThen(new PonAnnotation("somatic.pon", ponv2Resources.find("SOMATIC_PON.vcf.gz"), "SOMATIC_PON_COUNT"))
                .andThen(new StrelkaPostProcess(tumorSampleName, strelkaPostProcessBedResource.find("bed"), tumorBamPath))
                .andThen(new PonFilter())
                .andThen(new SageHotspotsAnnotation(sageResourceDownload.find("tsv"), sageOutput.outputFile().path()))
                .andThen(new SnpEff(snpEffResourceDownload.find("config")))
                .andThen(new DbSnpAnnotation(knownSnpsResourceDownload.find("vcf.gz")))
                .andThen(new CosmicAnnotation(cosmicResourceDownload.find("vcf.gz")))
                .apply(SubStageInputOutput.of(tumorSampleName, OutputFile.empty(), bash));

        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), namespacedResults.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(bash, namespacedResults));
        ImmutableSomaticCallerOutput.Builder output = SomaticCallerOutput.builder().status(status);
        if (status.equals(JobStatus.SUCCESS)) {
            output.finalSomaticVcf(GoogleStorageLocation.of(runtimeBucket.name(),
                    namespacedResults.path(mergedOutput.outputFile().fileName())));
        }
        return output.build();
    }

    @NotNull
    private Resource referenceGenomeResource() {
        return new Resource(storage,
                arguments.referenceGenomeBucket(),
                "reference_genome",
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
    }
}