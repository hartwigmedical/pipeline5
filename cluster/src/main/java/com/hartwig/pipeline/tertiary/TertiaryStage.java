package com.hartwig.pipeline.tertiary;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;

public abstract class TertiaryStage<S extends StageOutput> implements Stage<S, SomaticRunMetadata> {

    private final InputDownload tumorBamDownload;
    private final InputDownload tumorBaiDownload;
    private final InputDownload referenceBamDownload;
    private final InputDownload referenceBaiDownload;

    public TertiaryStage(final AlignmentPair alignmentPair) {
        tumorBamDownload = new InputDownload(alignmentPair.tumor().alignments());
        tumorBaiDownload = new InputDownload(alignmentPair.tumor().alignments().transform(FileTypes::toAlignmentIndex));
        referenceBamDownload = new InputDownload(alignmentPair.reference().alignments());
        referenceBaiDownload = new InputDownload(alignmentPair.reference().alignments().transform(FileTypes::toAlignmentIndex));
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
