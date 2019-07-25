package com.hartwig.pipeline.tertiary.purple;

import static com.google.common.collect.Lists.newArrayList;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

class PurpleApplicationCommand extends JavaJarCommand {
    PurpleApplicationCommand(String referenceSampleName, String tumorSampleName, String amberDirectory, String cobaltDirectory,
            String gcProfile, String somaticVcf, String structuralVcf, String svRecoveryVcf, String circosPath, boolean isShallow) {
        super("purple",
                Versions.PURPLE,
                "purple.jar",
                "8G",
                combine(arguments(referenceSampleName,
                        tumorSampleName,
                        amberDirectory,
                        cobaltDirectory,
                        gcProfile,
                        somaticVcf,
                        structuralVcf,
                        svRecoveryVcf,
                        circosPath), shallowArguments(isShallow)));
    }

    private static List<String> combine(List<String> first, List<String> second) {
        first.addAll(second);
        return first;
    }

    private static List<String> shallowArguments(final boolean isShallow) {
        if (isShallow) {
            return newArrayList("-highly_diploid_percentage", "0.88", "-somatic_min_total", "100", "-somatic_min_purity_spread", "0.1");
        }
        return new ArrayList<>();
    }

    @NotNull
    private static ArrayList<String> arguments(final String referenceSampleName, final String tumorSampleName, final String amberDirectory,
            final String cobaltDirectory, final String gcProfile, final String somaticVcf, final String structuralVcf,
            final String svRecoveryVcf, final String circosPath) {
        return newArrayList("-reference",
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
                Bash.allCpus());
    }
}
