package hmf.pipeline.adam;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import hmf.pipeline.Configuration;

class BwaCommand {

    static List<String> tokens(Configuration configuration) {
        List<String> cmd = new ArrayList<>();
        cmd.add("bwa");
        cmd.add("mem");
        cmd.add("-R");
        cmd.add(format("@RG\\tID:%s\\tLB:%s\\tPL:ILLUMINA\\tPU:0\\tSM:%s",
                configuration.sampleName(),
                configuration.sampleName(),
                configuration.sampleName()));
        cmd.add("-p");
        cmd.add(configuration.referencePath());
        cmd.add("-");
        return cmd;
    }
}
