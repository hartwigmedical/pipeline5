package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SageGermlineCaller extends SageCaller {

    public static final String NAMESPACE = "sage_germline";

    private final ResourceFiles resourceFiles;

    public SageGermlineCaller(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles,
            final PersistedDataset persistedDataset) {
        super(alignmentPair, persistedDataset, DataType.GERMLINE_VARIANTS_SAGE);
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

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).germlineMode(referenceSampleName, referenceBamPath, tumorSampleName, tumorBamPath)
                        .addCoverage()
                        .maxHeap("15G");
        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SageGermlinePostProcess sagePostProcess = new SageGermlinePostProcess(referenceSampleName, tumorSampleName, resourceFiles);

        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(sageOutput.bash());

        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.sageGermlineCalling(bash, resultsDirectory);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSageGermlineCaller();
    }

    @Override
    protected String filteredOutput(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s",
                metadata.tumor().sampleName(),
                SageGermlinePostProcess.SAGE_GERMLINE_FILTERED,
                FileTypes.GZIPPED_VCF);
    }

    @Override
    protected String unfilteredOutput(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s", metadata.tumor().sampleName(), "sage.germline", FileTypes.GZIPPED_VCF);
    }

    @Override
    protected ImmutableSageOutput.Builder outputBuilder(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        final String geneCoverageFile = String.format("%s.sage.gene.coverage.tsv", metadata.reference().sampleName());
        return super.outputBuilder(metadata, jobStatus, bucket, resultsDirectory)
                .addReportComponents(singleFileComponent(geneCoverageFile, bucket, resultsDirectory));
    }
}