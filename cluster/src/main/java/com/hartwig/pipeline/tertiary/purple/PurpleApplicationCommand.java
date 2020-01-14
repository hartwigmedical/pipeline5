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

    private static final String LOW_COVERAGE_DIPLOID_PERCENTAGE = "0.88";
    private static final String LOW_COVERAGE_SOMATIC_MIN_TOTAL = "100";
    private static final String LOW_COVERAGE_SOMATIC_MIN_PURITY_SPREAD = "0.1";

    PurpleApplicationCommand(String referenceSampleName, String tumorSampleName, String amberDirectory, String cobaltDirectory,
            String gcProfile, String somaticVcf, String structuralVcf, String svRecoveryVcf, String circosPath, String referenceGenomePath, boolean isShallow) {
        super("purple",
                Versions.PURPLE,
                "purple.jar",
                "12G",
                combine(arguments(referenceSampleName,
                        tumorSampleName,
                        amberDirectory,
                        cobaltDirectory,
                        gcProfile,
                        somaticVcf,
                        structuralVcf,
                        svRecoveryVcf,
                        circosPath,
                        referenceGenomePath), maybeShallowArguments(isShallow)));
    }

    @NotNull
    private static ArrayList<String> arguments(final String referenceSampleName, final String tumorSampleName, final String amberDirectory,
            final String cobaltDirectory, final String gcProfile, final String somaticVcf, final String structuralVcf,
            final String svRecoveryVcf, final String circosPath, String referenceGenomePath) {
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
                "-ref_genome",
                referenceGenomePath,
                "-threads",
                Bash.allCpus());
    }

    private static List<String> maybeShallowArguments(final boolean isShallow) {
        if (isShallow) {
            return newArrayList("-highly_diploid_percentage",
                    LOW_COVERAGE_DIPLOID_PERCENTAGE,
                    "-somatic_min_total",
                    LOW_COVERAGE_SOMATIC_MIN_TOTAL,
                    "-somatic_min_purity_spread",
                    LOW_COVERAGE_SOMATIC_MIN_PURITY_SPREAD);
        }
        return new ArrayList<>();
    }

    private static List<String> combine(List<String> first, List<String> second) {
        first.addAll(second);
        return first;
    }
}
