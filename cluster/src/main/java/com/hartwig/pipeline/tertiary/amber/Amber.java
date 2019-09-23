package com.hartwig.pipeline.tertiary.amber;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class Amber extends TertiaryStage<AmberOutput> {

    public static final String NAMESPACE = "amber";

    public Amber(final AlignmentPair alignmentPair) {
        super(alignmentPair);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(storage, resourceBucket, ResourceNames.REFERENCE_GENOME, bucket),
                ResourceDownload.from(storage, resourceBucket, ResourceNames.AMBER_PON, bucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        return Collections.singletonList(new AmberApplicationCommand(metadata.reference().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath(),
                resources.get(ResourceNames.REFERENCE_GENOME).find("fasta"),
                resources.get(ResourceNames.AMBER_PON).find("GermlineHetPon.hg19.bed"),
                resources.get(ResourceNames.AMBER_PON).find("GermlineSnp.hg19.bed")));
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
