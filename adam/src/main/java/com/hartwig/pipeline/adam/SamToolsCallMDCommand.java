package com.hartwig.pipeline.adam;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.patient.ReferenceGenome;

class SamToolsCallMDCommand {

    static List<String> tokens(ReferenceGenome referenceGenome) {
        List<String> cmd = new ArrayList<>();
        cmd.add("samtools");
        cmd.add("calmd");
        cmd.add("-b");
        cmd.add("-");
        cmd.add(referenceGenome.path());
        return cmd;
    }
}
