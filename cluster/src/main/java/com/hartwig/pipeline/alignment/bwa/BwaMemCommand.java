package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.computeengine.execution.vm.Bash;

class BwaMemCommand extends BwaCommand {

    private static final String SAMPLE_NAME = "NA";

    BwaMemCommand(final String recordGroupId, final String flowcellId, final String referenceGenomePath,
            final String first, final String second) {
        super("mem",
                "-R",
                format("\"@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:%s\\tSM:%s\"", recordGroupId, SAMPLE_NAME, flowcellId, SAMPLE_NAME),
                "-Y",
                "-t",
                Bash.allCpus(),
                referenceGenomePath,
                first,
                second);
    }
}