package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PON;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.STRELKA_CONFIG;

import java.util.List;

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
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class SomaticCaller extends TertiaryStage<SomaticCallerOutput> {

    public static final String NAMESPACE = "somatic_caller";

    private final ResourceFiles resourceFiles;
    private OutputFile outputFile;
    private OutputFile sageOutputFile;

    public SomaticCaller(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles)
    {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        List<BashCommand> commands = Lists.newArrayList();

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();
        String knownHotspotsTsv = ResourceFiles.of(SAGE, "KnownHotspots.tsv");
        SageHotspotApplication sageHotspotApplication = new SageHotspotApplication(knownHotspotsTsv,
                ResourceFiles.of(SAGE, "CodingRegions.bed"),
                resourceFiles.refGenomeFile(),
                tumorBamPath,
                referenceBamPath,
                tumorSampleName,
                referenceSampleName);
        sageOutputFile = sageHotspotApplication.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();
        SubStageInputOutput sageOutput = sageHotspotApplication.andThen(new SageFiltersAndAnnotations(tumorSampleName))
                .andThen(new PonAnnotation("sage.hotspots.pon", ResourceFiles.of(SAGE, "SAGE_PON.vcf.gz"), "SAGE_PON_COUNT"))
                .andThen(new SagePonFilter())
                .apply(SubStageInputOutput.empty(tumorSampleName));

        commands.addAll(sageOutput.bash());

        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        SubStageInputOutput mergedOutput = new Strelka(referenceBamPath,
                tumorBamPath, ResourceFiles.of(STRELKA_CONFIG, "strelka_config_bwa_genome.ini"), resourceFiles.refGenomeFile())
                .andThen(new MappabilityAnnotation(resourceFiles.out150Mappability(), ResourceFiles.of(MAPPABILITY, "mappability.hdr")))
                .andThen(new PonAnnotation("germline.pon", ResourceFiles.of(PON, "GERMLINE_PON.vcf.gz"), "GERMLINE_PON_COUNT"))
                .andThen(new PonAnnotation("somatic.pon", ResourceFiles.of(PON, "SOMATIC_PON.vcf.gz"), "SOMATIC_PON_COUNT"))
                .andThen(new StrelkaPostProcess(tumorSampleName, resourceFiles.giabHighConfidenceBed(), tumorBamPath))
                .andThen(new PonFilter())
                .andThen(new SageHotspotAnnotation(knownHotspotsTsv, sageOutput.outputFile().path()))
                .andThen(new SnpEff(ResourceFiles.SNPEFF_CONFIG, resourceFiles))
                .andThen(FinalSubStage.of(new CosmicAnnotation(ResourceFiles.COSMIC_VCF_GZ, "ID,INFO")))
                .apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(mergedOutput.bash());

        outputFile = mergedOutput.outputFile();
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.somaticCalling(bash, resultsDirectory);
    }

    @Override
    public SomaticCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SomaticCallerOutput.builder(NAMESPACE)
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        outputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "somatic_caller_post_processed", OutputFile.GZIPPED_VCF, false)
                                .fileName(),
                        resultsDirectory))
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
        return SomaticCallerOutput.builder(NAMESPACE).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSomaticCaller();
    }
}