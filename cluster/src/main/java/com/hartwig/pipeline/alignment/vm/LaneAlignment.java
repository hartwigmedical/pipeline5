package com.hartwig.pipeline.alignment.vm;

import com.hartwig.patient.Lane;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class LaneAlignment extends SubStage {

    private final String referenceGenomePath;
    private final String firstFastqPath;
    private final String secondFastqPath;
    private final String sampleName;
    private final Lane lane;

    LaneAlignment(final String referenceGenomePath, final String firstFastqPath, final String secondFastqPath, final String sampleName,
            final Lane lane) {
        super("sorted." + VmAligner.laneId(lane), OutputFile.BAM);
        this.referenceGenomePath = referenceGenomePath;
        this.firstFastqPath = firstFastqPath;
        this.secondFastqPath = secondFastqPath;
        this.sampleName = sampleName;
        this.lane = lane;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new PipeCommands(new BwaMemCommand(lane.recordGroupId(),
                sampleName,
                lane.flowCellId(),
                referenceGenomePath,
                firstFastqPath,
                secondFastqPath), new SambambaViewCommand(), new SambambaSortCommand(output.path(), "/dev/stdin")));
    }
}
