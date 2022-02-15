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
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SageSomaticCaller extends SageCaller {

    public static final String NAMESPACE = "sage_somatic";

    private final ResourceFiles resourceFiles;
    private final boolean shallow;

    public SageSomaticCaller(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset,
            boolean shallow) {
        super(alignmentPair,
                persistedDataset,
                DataType.SOMATIC_VARIANTS_SAGE,
                DataType.SOMATIC_GENE_COVERAGE,
                DataType.SOMATIC_TUMOR_SAMPLE_BQR_PLOT,
                DataType.SOMATIC_REF_SAMPLE_BQR_PLOT);
        this.resourceFiles = resourceFiles;
        this.shallow = shallow;
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

        SageCommandBuilder sageCommandBuilder = new SageCommandBuilder(resourceFiles).addReference(referenceSampleName, referenceBamPath)
                .addTumor(tumorSampleName, tumorBamPath)
                .addCoverage();
        if (shallow) {
            sageCommandBuilder = sageCommandBuilder.addShallowSomaticMode();
        }

        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SageSomaticPostProcess sagePostProcess = new SageSomaticPostProcess(tumorSampleName, resourceFiles);

        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(sageOutput.bash());

        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.sageSomaticCalling(bash, resultsDirectory);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSomaticCaller();
    }

    @Override
    protected String filteredOutput(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s",
                metadata.tumor().sampleName(),
                SageSomaticPostProcess.SAGE_SOMATIC_FILTERED,
                FileTypes.GZIPPED_VCF);
    }

    @Override
    protected String unfilteredOutput(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s", metadata.tumor().sampleName(), "sage.somatic", FileTypes.GZIPPED_VCF);
    }

    @Override
    protected ImmutableSageOutput.Builder outputBuilder(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        final String geneCoverageFile = String.format("%s.%s", metadata.tumor().sampleName(), SAGE_GENE_COVERAGE_TSV);
        return super.outputBuilder(metadata, jobStatus, bucket, resultsDirectory)
                .addReportComponents(singleFileComponent(geneCoverageFile, bucket, resultsDirectory));
    }
}