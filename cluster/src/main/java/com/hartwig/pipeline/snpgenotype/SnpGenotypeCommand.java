package com.hartwig.pipeline.snpgenotype;

import com.hartwig.pipeline.execution.vm.ParallelGatkCommand;

class SnpGenotypeCommand extends ParallelGatkCommand {

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
                "--output_mode",
                "EMIT_ALL_SITES"
        );
    }
}
