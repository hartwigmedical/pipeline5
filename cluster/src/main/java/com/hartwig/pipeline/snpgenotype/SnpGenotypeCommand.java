package com.hartwig.pipeline.snpgenotype;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.GatkCommand;

class SnpGenotypeCommand extends GatkCommand {

    SnpGenotypeCommand(String inputBam, String referenceFasta, String genotypeSnpsDb, String outputVcf) {
        super("20G",
                "UnifiedGenotyper",
                "--input_file",
                inputBam,
                "-o",
                outputVcf,
                "-L",
                genotypeSnpsDb,
                "--reference_sequence",
                referenceFasta,
                "-nct", Bash.allCpus(),
                "--output_mode EMIT_ALL_SITES"
        );
    }
}
