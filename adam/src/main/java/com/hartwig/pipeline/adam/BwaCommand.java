package com.hartwig.pipeline.adam;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.patient.Lane;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;

class BwaCommand {

    static List<String> tokens(ReferenceGenome referenceGenome, Sample sample, Lane lane) {
        List<String> cmd = new ArrayList<>();
        cmd.add("bwa");
        cmd.add("mem");
        cmd.add("-R");
        cmd.add(format("@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:0\\tSM:%s", sample.name(), sample.name(), sample.name()));
        cmd.add("-c");
        cmd.add("100");
        cmd.add("-M");
        cmd.add("-t");
        cmd.add("12");
        cmd.add(referenceGenome.path());
        cmd.add(lane.readsFile());
        cmd.add("-");
        return cmd;
    }
}
