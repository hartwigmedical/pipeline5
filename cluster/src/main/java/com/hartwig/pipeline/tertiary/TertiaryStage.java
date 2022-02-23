package com.hartwig.pipeline.tertiary;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public abstract class TertiaryStage<S extends StageOutput> implements Stage<S, SomaticRunMetadata> {

    private final InputDownload tumorBamDownload;
    private final InputDownload tumorBaiDownload;
    private final InputDownload referenceBamDownload;
    private final InputDownload referenceBaiDownload;

    public TertiaryStage(final AlignmentPair alignmentPair) {
        tumorBamDownload =
                new InputDownload(alignmentPair.maybeTumor().map(AlignmentOutput::finalBamLocation).orElse(GoogleStorageLocation.empty()));
        tumorBaiDownload =
                new InputDownload(alignmentPair.maybeTumor().map(AlignmentOutput::finalBaiLocation).orElse(GoogleStorageLocation.empty()));
        referenceBamDownload = new InputDownload(alignmentPair.maybeReference()
                .map(AlignmentOutput::finalBamLocation)
                .orElse(GoogleStorageLocation.empty()));
        referenceBaiDownload = new InputDownload(alignmentPair.maybeReference()
                .map(AlignmentOutput::finalBaiLocation)
                .orElse(GoogleStorageLocation.empty()));
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(tumorBamDownload, tumorBaiDownload, referenceBamDownload, referenceBaiDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    protected InputDownload getTumorBamDownload() {
        return tumorBamDownload;
    }

    protected InputDownload getReferenceBamDownload() {
        return referenceBamDownload;
    }
}
