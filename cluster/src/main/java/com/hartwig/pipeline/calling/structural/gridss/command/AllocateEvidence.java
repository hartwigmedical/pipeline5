package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.Collections;

import com.hartwig.pipeline.execution.vm.Bash;

public class AllocateEvidence extends GridssCommand {

    public AllocateEvidence(final String normalBam, final String tumorBam, final String assemblyBam, final String inputVcf,
            final String outputVcf,  final String referencesSequence, final String gridssConfigPath) {
        super("gridss.AllocateEvidence",
                "8G",
                Collections.emptyList(),
                "ALLOCATE_READS=false",
                "REFERENCE_SEQUENCE=" + referencesSequence,
                "I=" + normalBam,
                "I=" + tumorBam,
                "ASSEMBLY=" + assemblyBam,
                "INPUT_VCF=" + inputVcf,
                "OUTPUT_VCF=" + outputVcf,
                "CONFIGURATION_FILE=" + gridssConfigPath,
                "WORKER_THREADS=" + Bash.allCpus());
    }
}
