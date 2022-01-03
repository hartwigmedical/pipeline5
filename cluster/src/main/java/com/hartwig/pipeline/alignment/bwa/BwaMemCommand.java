package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.execution.vm.Bash;

class BwaMemCommand extends BwaCommand {

    BwaMemCommand(final String recordGroupId, final String sampleName, final String flowcellId, final String referenceGenomePath,
            final String first, final String second) {
        super("mem",
                "-R",
                format("\"@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:%s\\tSM:%s\"", recordGroupId, sampleName, flowcellId, sampleName),
                "-Y",
                "-t",
                Bash.allCpus(),
                "-K",
                "10000000",
                referenceGenomePath,
                first,
                second);
    }
}