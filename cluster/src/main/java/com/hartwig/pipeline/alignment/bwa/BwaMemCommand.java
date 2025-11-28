package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import com.hartwig.computeengine.execution.vm.Bash;

class BwaMemCommand extends BwaMem2Command {

    BwaMemCommand(final String recordGroupId, final String sampleName, final String flowcellId, final String referenceGenomePath,
            final String first, final String second) {
        super("mem",
                "-R",
                format("\"@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:%s\\tSM:%s\"", recordGroupId, sampleName, flowcellId, sampleName),
                "-Y",
                "-t",
                Bash.allCpus(),
                referenceGenomePath,
                first,
                second);
    }
}