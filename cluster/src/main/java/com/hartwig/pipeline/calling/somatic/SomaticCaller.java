package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PON;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;
import static com.hartwig.pipeline.resource.ResourceNames.STRELKA_CONFIG;

import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.FinalSubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

import org.jetbrains.annotations.NotNull;

public class SomaticCaller extends TertiaryStage<SomaticCallerOutput> {

    public static final String NAMESPACE = "somatic_caller";

    private OutputFile outputFile;
    private OutputFile sageOutputFile;

    public SomaticCaller(final AlignmentPair alignmentPair) {
        super(alignmentPair);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket runtimeBucket) {
        return Lists.newArrayList(ResourceDownload.from(runtimeBucket, referenceGenomeResource(storage, resourceBucket)),
                ResourceDownload.from(storage, resourceBucket, STRELKA_CONFIG, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, MAPPABILITY, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, PON, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, BEDS, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, SAGE, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, SNPEFF, runtimeBucket),
                ResourceDownload.from(storage, resourceBucket, COSMIC, runtimeBucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata, final Map<String, ResourceDownload> resources) {

        List<BashCommand> commands = Lists.newArrayList();

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String referenceGenomePath = resources.get(REFERENCE_GENOME).find("fasta");
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();
        ResourceDownload sageResources = resources.get(SAGE);
        SubStageInputOutput sageOutput = new SageHotspotsApplication(sageResources.find("tsv"),
                sageResources.find("bed"),
                referenceGenomePath,
                tumorBamPath,
                referenceBamPath,
                tumorSampleName,
                referenceSampleName).andThen(new SageFiltersAndAnnotations(tumorSampleName))
                .andThen(new SagePonAnnotation(sageResources.find("SAGE_PON.vcf.gz")))
                .andThen(new SagePonFilter())
                .apply(SubStageInputOutput.empty(tumorSampleName));

        commands.addAll(sageOutput.bash());

        ResourceDownload snpEffResources = resources.get(SNPEFF);
        String snpEffDb = snpEffResources.find("zip");
        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, snpEffDb));

        ResourceDownload mappabilityResources = resources.get(MAPPABILITY);
        ResourceDownload ponV2Resources = resources.get(PON);
        SubStageInputOutput mergedOutput = new Strelka(referenceBamPath,
                tumorBamPath,
                resources.get(STRELKA_CONFIG).find("ini"),
                referenceGenomePath).andThen(new MappabilityAnnotation(mappabilityResources.find("bed.gz"),
                mappabilityResources.find("hdr")))
                .andThen(new PonAnnotation("germline.pon", ponV2Resources.find("GERMLINE_PON.vcf.gz"), "GERMLINE_PON_COUNT"))
                .andThen(new PonAnnotation("somatic.pon", ponV2Resources.find("SOMATIC_PON.vcf.gz"), "SOMATIC_PON_COUNT"))
                .andThen(new StrelkaPostProcess(tumorSampleName, resources.get(BEDS).find("bed"), tumorBamPath))
                .andThen(new PonFilter())
                .andThen(new SageHotspotsAnnotation(sageResources.find("tsv"), sageOutput.outputFile().path()))
                .andThen(new SnpEff(snpEffResources.find("config")))
                .andThen(FinalSubStage.of(new CosmicAnnotation(resources.get(COSMIC).find("collapsed.vcf.gz"), "ID,INFO")))
                .apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(mergedOutput.bash());

        outputFile = mergedOutput.outputFile();
        sageOutputFile = sageOutput.outputFile();
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.somaticCalling(bash, resultsDirectory);
    }

    @Override
    public SomaticCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SomaticCallerOutput.builder()
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        outputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "somatic_caller_post_processed", OutputFile.GZIPPED_VCF, false)
                                .fileName(), resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        sageOutputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage_hotspots", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.from(),
                        NAMESPACE,
                        "strelkaAnalysis/",
                        resultsDirectory,
                        s -> s.contains("chromosomes") || s.contains("Makefile") || s.contains("task.complete")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))
                .build();
    }

    @Override
    public SomaticCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SomaticCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSomaticCaller();
    }

    @NotNull
    private Resource referenceGenomeResource(final Storage storage, final String resourceBucket) {
        return new Resource(storage, resourceBucket, REFERENCE_GENOME);
    }
}