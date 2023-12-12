package com.hartwig.pipeline.tertiary;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;

public abstract class TertiaryStage<S extends StageOutput> implements Stage<S, SomaticRunMetadata> {

    private final InputDownloadCommand tumorBamDownload;
    private final InputDownloadCommand tumorBaiDownload;
    private final InputDownloadCommand referenceBamDownload;
    private final InputDownloadCommand referenceBaiDownload;

    public TertiaryStage(final AlignmentPair alignmentPair) {
        tumorBamDownload = new InputDownloadCommand(alignmentPair.tumor().alignments());
        tumorBaiDownload = new InputDownloadCommand(alignmentPair.tumor().alignments().transform(FileTypes::toAlignmentIndex));
        referenceBamDownload = new InputDownloadCommand(alignmentPair.reference().alignments());
        referenceBaiDownload = new InputDownloadCommand(alignmentPair.reference().alignments().transform(FileTypes::toAlignmentIndex));
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(tumorBamDownload, tumorBaiDownload, referenceBamDownload, referenceBaiDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    protected InputDownloadCommand getTumorBamDownload() {
        return tumorBamDownload;
    }

    protected InputDownloadCommand getReferenceBamDownload() {
        return referenceBamDownload;
    }
}
