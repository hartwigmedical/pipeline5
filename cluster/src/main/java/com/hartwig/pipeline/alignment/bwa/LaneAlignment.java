package com.hartwig.pipeline.alignment.bwa;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.unix.PipeCommands;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class LaneAlignment extends SubStage {

    private final boolean strictFastqNaming;
    private final String referenceGenomePath;
    private final String firstFastqPath;
    private final String secondFastqPath;
    private final String sampleName;
    private final LaneInput lane;

    LaneAlignment(final boolean strictFastqNaming, final String referenceGenomePath, final String firstFastqPath,
            final String secondFastqPath, final String sampleName, final LaneInput lane) {
        super(BwaAligner.laneId(lane), FileTypes.BAM);
        this.strictFastqNaming = strictFastqNaming;
        this.referenceGenomePath = referenceGenomePath;
        this.firstFastqPath = firstFastqPath;
        this.secondFastqPath = secondFastqPath;
        this.sampleName = sampleName;
        this.lane = lane;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new PipeCommands(new BwaMemCommand(lane.readGroup()
                .map(rg -> String.format("\"%s\"", rg))
                .orElse(ReadGroup.fromFastq(sampleName, lane.flowCellId(), strictFastqNaming, firstFastqPath)),
                referenceGenomePath,
                firstFastqPath,
                secondFastqPath), new SamtoolsViewCommand(), new SambambaSortCommand(output.path(), "/dev/stdin")));
    }
}
