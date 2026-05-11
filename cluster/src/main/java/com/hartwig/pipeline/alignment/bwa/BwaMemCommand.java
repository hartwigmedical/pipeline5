package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.computeengine.execution.vm.Bash;

class BwaMemCommand extends BwaMem2Command {

    BwaMemCommand(final String readGroup, final String referenceGenomePath, final String first, final String second) {
        super("mem", "-R", readGroup, "-Y", "-t", Bash.allCpus(), referenceGenomePath, first, second);
    }
}