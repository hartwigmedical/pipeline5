package com.hartwig.pipeline.calling.germline.command;

import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.execution.vm.ParallelGatkCommand;

public class GatkHaplotypeCallerCommand extends ParallelGatkCommand {

    public GatkHaplotypeCallerCommand(String inputBam, String referenceFasta, String knownSnpsDb, String outputVcf) {
        super(GermlineCaller.TOOL_HEAP,
                "HaplotypeCaller",
                "--input_file",
                inputBam,
                "-o",
                outputVcf,
                "-D",
                knownSnpsDb,
                "--reference_sequence",
                referenceFasta,
                "-variant_index_type",
                "LINEAR",
                "-variant_index_parameter",
                "128000",
                "-stand_call_conf",
                "15.0",
                "-ERC GVCF",
                "-GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60",
                "--sample_ploidy",
                "2");
    }
}
