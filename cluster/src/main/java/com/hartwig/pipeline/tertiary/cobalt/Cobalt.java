package com.hartwig.pipeline.tertiary.cobalt;

import java.util.Collections;
import java.util.List;

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
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class Cobalt extends TertiaryStage<CobaltOutput> {

    public static final String NAMESPACE = "cobalt";
    private final Resource resource;

    public Cobalt(final AlignmentPair alignmentPair, final Resource resource)
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
        return Collections.singletonList(new CobaltApplicationCommand(metadata.reference().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath(),
                resource.gcProfileFile()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.cobalt(bash, resultsDirectory);
    }

    @Override
    public CobaltOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return CobaltOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public CobaltOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder().status(PipelineStatus.SKIPPED).build();
    }
}
