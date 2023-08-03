package com.hartwig.pipeline.alignment.bwa;

import java.util.Collections;
import java.util.List;

import com.hartwig.pdl.LaneInput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.command.unix.PipeCommands;
import com.hartwig.pipeline.stages.SubStage;

public class LaneAlignment extends SubStage {

    private final boolean strictFastqNaming;
    private final String referenceGenomePath;
    private final String firstFastqPath;
    private final String secondFastqPath;
    private final LaneInput lane;

    LaneAlignment(final boolean strictFastqNaming, final String referenceGenomePath, final String firstFastqPath, final String secondFastqPath,
            final LaneInput lane) {
        super(BwaAligner.laneId(lane), FileTypes.BAM);
        this.strictFastqNaming = strictFastqNaming;
        this.referenceGenomePath = referenceGenomePath;
        this.firstFastqPath = firstFastqPath;
        this.secondFastqPath = secondFastqPath;
        this.lane = lane;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new PipeCommands(new BwaMemCommand(RecordGroupId.from(strictFastqNaming, firstFastqPath),
                lane.flowCellId(),
                referenceGenomePath,
                firstFastqPath,
                secondFastqPath), new SamtoolsViewCommand(), new SambambaSortCommand(output.path(), "/dev/stdin")));
    }
}
