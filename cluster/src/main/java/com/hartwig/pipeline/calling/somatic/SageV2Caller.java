package com.hartwig.pipeline.calling.somatic;

import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.FinalSubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
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
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class SageV2Caller extends TertiaryStage<SomaticCallerOutput> {

    public static final String NAMESPACE = "sage";

    private final ResourceFiles resourceFiles;
    private OutputFile outputFile;
    private OutputFile sageOutputFile;

    public SageV2Caller(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles) {
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
        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();

        SageV2Application sageV2Application =
                new SageV2Application(resourceFiles, tumorBamPath, referenceBamPath, tumorSampleName, referenceSampleName);
        sageOutputFile = sageV2Application.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();

        final String refGenomeStr = resourceFiles.version() == RefGenomeVersion.HG37 ? "hg19" : "hg38";

        SubStageInputOutput sageOutput = sageV2Application.andThen(new SageV2PassFilter())
                .andThen(new MappabilityAnnotation(resourceFiles.out150Mappability(), ResourceFiles.of(MAPPABILITY, "mappability.hdr")))
                .andThen(new PonAnnotation("sage.pon", resourceFiles.sageGermlinePon(), "PON_COUNT", "PON_MAX"))
                .andThen(new SageV2PonFilter())
                .andThen(new SnpEff(ResourceFiles.SNPEFF_CONFIG, resourceFiles))
                .andThen(FinalSubStage.of(new SageV2PostProcess(refGenomeStr)))
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
    public SomaticCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SomaticCallerOutput.builder(NAMESPACE)
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        outputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage.somatic.post_processed", OutputFile.GZIPPED_VCF, false)
                                .fileName(),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        sageOutputFile.fileName(),
                        OutputFile.of(metadata.tumor().sampleName(), "sage.somatic", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
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
        return !arguments.shallow() && arguments.runSageCaller();
    }
}