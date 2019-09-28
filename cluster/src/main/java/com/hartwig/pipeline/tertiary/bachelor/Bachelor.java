package com.hartwig.pipeline.tertiary.bachelor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Bachelor implements Stage<BachelorOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "bachelor";
    private final InputDownload purpleOutputDownload;
    private final InputDownload tumorBamDownload;
    private final InputDownload tumorBaiDownload;
    private final InputDownload germlineVcfDownload;

    public Bachelor(final PurpleOutput purpleOutput, AlignmentOutput tumorAlignmentOutput, GermlineCallerOutput germlineCallerOutput) {
        this.purpleOutputDownload = new InputDownload(purpleOutput.outputDirectory());
        this.tumorBamDownload = new InputDownload(tumorAlignmentOutput.finalBamLocation());
        this.tumorBaiDownload = new InputDownload(tumorAlignmentOutput.finalBaiLocation());
        this.germlineVcfDownload = new InputDownload(germlineCallerOutput.germlineVcfLocation());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(purpleOutputDownload, tumorBamDownload, tumorBaiDownload, germlineVcfDownload);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(storage, resourceBucket, ResourceNames.REFERENCE_GENOME, bucket),
                ResourceDownload.from(storage, resourceBucket, ResourceNames.BACHELOR, bucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        ResourceDownload bachelorResources = resources.get(ResourceNames.BACHELOR);
        return Collections.singletonList(new BachelorCommand(metadata.tumor().sampleName(),
                germlineVcfDownload.getLocalTargetPath(),
                tumorBamDownload.getLocalTargetPath(),
                purpleOutputDownload.getLocalTargetPath(),
                bachelorResources.find("bachelor_hmf.xml"),
                bachelorResources.find("bachelor_clinvar_filters.csv"),
                resources.get(ResourceNames.REFERENCE_GENOME).find("fasta"),
                VmDirectories.OUTPUT));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.bachelor(bash, resultsDirectory);
    }

    @Override
    public BachelorOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return BachelorOutput.builder().status(jobStatus).build();
    }

    @Override
    public BachelorOutput skippedOutput(final SomaticRunMetadata metadata) {
        return BachelorOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
