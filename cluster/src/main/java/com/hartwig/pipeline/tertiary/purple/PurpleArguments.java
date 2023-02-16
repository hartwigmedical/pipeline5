package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.*;

import java.util.List;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

final class PurpleArguments {
    private static final String CIRCOS_PATH = VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos";

    public static List<String> tumorArguments(final String tumorSampleName, final String somaticVcfPath, final String structuralVcfPath,
            final String svRecoveryVcfPath, final ResourceFiles resourceFiles) {
        return List.of(format("-tumor %s", tumorSampleName),
                format("-somatic_vcf %s", somaticVcfPath),
                format("-structural_vcf %s", structuralVcfPath),
                format("-sv_recovery_vcf %s", svRecoveryVcfPath),
                format("-somatic_hotspots %s", resourceFiles.sageSomaticHotspots()),
                format("-circos %s", CIRCOS_PATH));
    }

    public static List<String> germlineArguments(final String sampleName, final String germlineVcfPath, final ResourceFiles resourceFiles) {
        return List.of(format("-reference %s", sampleName),
                format("-germline_vcf %s", germlineVcfPath),
                format("-germline_hotspots %s", resourceFiles.sageGermlineHotspots()),
                format("-germline_del_freq_file %s", resourceFiles.purpleCohortGermlineDeletions()));
    }


    public static List<String> addTargetRegionsArguments(final ResourceFiles resourceFiles) {
        return List.of(format("-target_regions_bed %s", resourceFiles.targetRegionsBed()),
                format("-target_regions_ratios %s", resourceFiles.targetRegionsRatios()),
                format("-target_regions_msi_indels %s", resourceFiles.targetRegionsMsiIndels()),
                "-min_diploid_tumor_ratio_count 3",
                "-min_diploid_tumor_ratio_count_centromere 3");
    }

    public static List<String> addCommonArguments(final String amberOutputPath, final String cobaltOutputPath,
            final ResourceFiles resourceFiles) {
        return List.of(format("-amber %s", amberOutputPath),
                format("-cobalt %s", cobaltOutputPath),
                format("-ref_genome %s", resourceFiles.refGenomeFile()),
                format("-ref_genome_version %s", resourceFiles.version().toString()),
                format("-driver_gene_panel %s", resourceFiles.driverGenePanel()),
                format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()),
                format("-gc_profile %s", resourceFiles.gcProfileFile()),
                format("-output_dir %s", VmDirectories.OUTPUT),
                format("-threads %s", Bash.allCpus()));
    }

    public static void addShallowArguments(final List<String> arguments) {
        arguments.add("-highly_diploid_percentage 0.88");
        arguments.add("-somatic_min_purity_spread 0.1");
    }
}

