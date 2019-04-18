package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.execution.vm.GatkCommand;

class GatkHaplotypeCaller extends GatkCommand {

    GatkHaplotypeCaller(String inputBam, String referenceFasta, String outputVcf) {
        super("20G",
                "HaplotypeCaller",
                "--input_file",
                inputBam,
                "-o",
                outputVcf,
                "--reference_sequence",
                referenceFasta,
                "-nct",
                "$(grep -c '^processor' /proc/cpuinfo)",
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
