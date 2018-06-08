package hmf.pipeline.adam;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import hmf.patient.Reference;
import hmf.patient.Sample;

class BwaCommand {

    static List<String> tokens(Reference reference, Sample sample) {
        List<String> cmd = new ArrayList<>();
        cmd.add("bwa");
        cmd.add("mem");
        cmd.add("-R");
        cmd.add(format("@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:0\\tSM:%s", sample.name(), sample.name(), sample.name()));
        cmd.add("-p");
        cmd.add(reference.path());
        cmd.add("-");
        return cmd;
    }
}
