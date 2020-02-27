package com.hartwig.pipeline.tertiary.amber;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class Amber extends TertiaryStage<AmberOutput> {

    public static final String NAMESPACE = "amber";

    private final Resource resource;

    public Amber(final AlignmentPair alignmentPair, final Resource resource)
    {
        super(alignmentPair);
        this.resource = resource;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new AmberApplicationCommand(metadata.reference().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath(),
                resource.refGenomeFile(),
                resource.germlineHetPon()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.amber(bash, resultsDirectory);
    }

    @Override
    public AmberOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return AmberOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public AmberOutput skippedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
