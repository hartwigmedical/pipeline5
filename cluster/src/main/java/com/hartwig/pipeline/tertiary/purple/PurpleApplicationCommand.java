package com.hartwig.pipeline.tertiary.purple;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

class PurpleApplicationCommand extends JavaJarCommand {
    PurpleApplicationCommand(String referenceSampleName, String tumorSampleName, String amberDirectory, String cobaltDirectory,
            String gcProfile, String somaticVcf, String structuralVcf, String svRecoveryVcf, String circosPath) {
        super("purple",
                Versions.PURPLE,
                "purple.jar",
                "8G",
                Lists.newArrayList("-reference",
                        referenceSampleName,
                        "-tumor",
                        tumorSampleName,
                        "-output_dir",
                        VmDirectories.OUTPUT,
                        "-amber",
                        amberDirectory,
                        "-cobalt",
                        cobaltDirectory,
                        "-gc_profile",
                        gcProfile,
                        "-somatic_vcf",
                        somaticVcf,
                        "-structural_vcf",
                        structuralVcf,
                        "-sv_recovery_vcf",
                        svRecoveryVcf,
                        "-circos",
                        circosPath,
                        "-threads",
                        "16"));
    }
}
