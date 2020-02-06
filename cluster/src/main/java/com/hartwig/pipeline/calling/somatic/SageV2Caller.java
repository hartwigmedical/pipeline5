package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;

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
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class SageV2Caller extends TertiaryStage<SageV2CallerOutput> {

    public static final String NAMESPACE = "sage";

    private OutputFile outputFile;
    private OutputFile sageOutputFile;

    public SageV2Caller(final AlignmentPair alignmentPair) {
        super(alignmentPair);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        List<BashCommand> commands = Lists.newArrayList();
        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, Resource.SNPEFF_DB));

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String referenceGenomePath = Resource.REFERENCE_GENOME_FASTA;
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();

        SageV2Application sageV2Application = new SageV2Application(Resource.of(SAGE, "KnownHotspots.hg19.vcf.gz"),
                Resource.of(SAGE, "ActionableCodingPanel.hg19.bed.gz"),
                Resource.of(BEDS, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed"),
                referenceGenomePath,
                tumorBamPath,
                referenceBamPath,
                tumorSampleName,
                referenceSampleName);
        sageOutputFile = sageV2Application.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();

        SubStageInputOutput sageOutput = sageV2Application
                .andThen(new SageV2PassFilter(tumorSampleName))
                .andThen(new MappabilityAnnotation(Resource.of(MAPPABILITY, "out_150_hg19.mappability.bed.gz"), Resource.of(MAPPABILITY, "mappability.hdr")))
                .andThen(new PonAnnotation("sage.pon", Resource.of(SAGE, "SageGermlinePon.hg19.vcf.gz"), "PON_COUNT"))
                .andThen(new SageV2PonFilter())
                .andThen(new SnpEff(Resource.SNPEFF_CONFIG))
                .andThen(new SageV2PostProcess("hg19"))
                .andThen(FinalSubStage.of(new CosmicAnnotation(Resource.COSMIC_VCF_GZ, "ID,INFO")))
                .apply(SubStageInputOutput.empty(tumorSampleName));

        commands.addAll(sageOutput.bash());
        outputFile = sageOutput.outputFile();
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.sageCalling(bash, resultsDirectory);
    }

    @Override
    public SageV2CallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SageV2CallerOutput.builder()
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        outputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage_post_processed", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        sageOutputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))
                .build();
    }

    @Override
    public SageV2CallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SageV2CallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSageCaller();
    }
}