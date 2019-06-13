package com.hartwig.pipeline.calling.somatic;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.trace.StageTrace;

import org.jetbrains.annotations.NotNull;

public class SomaticCaller {

    static final String NAMESPACE = "somatic_caller";
    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {

        if (!arguments.runSomaticCaller()) {
            return SomaticCallerOutput.builder().status(JobStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload = ResourceDownload.from(runtimeBucket, referenceGenomeResource());
        String referenceGenomePath = referenceGenomeDownload.find("fasta");
        ResourceDownload strelkaConfigDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.STRELKA_CONFIG, runtimeBucket);
        ResourceDownload mappabilityResources =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.MAPPABILITY, runtimeBucket);
        ResourceDownload ponv2Resources = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.PON, runtimeBucket);
        ResourceDownload strelkaPostProcessBedResource =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.BEDS, runtimeBucket);
        ResourceDownload sageResourceDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.SAGE, runtimeBucket);
        ResourceDownload snpEffResourceDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.SNPEFF, runtimeBucket);
        ResourceDownload knownSnpsResourceDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.DBSNPS, runtimeBucket);
        ResourceDownload cosmicResourceDownload =
                ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.COSMIC, runtimeBucket);
        bash.addCommand(referenceGenomeDownload)
                .addCommand(strelkaConfigDownload)
                .addCommand(mappabilityResources)
                .addCommand(ponv2Resources)
                .addCommand(strelkaPostProcessBedResource)
                .addCommand(sageResourceDownload)
                .addCommand(snpEffResourceDownload)
                .addCommand(knownSnpsResourceDownload)
                .addCommand(cosmicResourceDownload);

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().finalBaiLocation());
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
                .andThen(new CosmicAnnotation(cosmicResourceDownload.find("vcf.gz"), "ID,INFO"))
                .apply(SubStageInputOutput.of(tumorSampleName, OutputFile.empty(), bash));

        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(bash, resultsDirectory));
        trace.stop();
        return SomaticCallerOutput.builder()
                .status(status)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(runtimeBucket.name(),
                        resultsDirectory.path(mergedOutput.outputFile().fileName())))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, pair, NAMESPACE, resultsDirectory, "chromosomes"))
                .build();

    }

    @NotNull
    private Resource referenceGenomeResource() {
        return new Resource(storage,
                arguments.resourceBucket(),
                ResourceNames.REFERENCE_GENOME,
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
    }
}