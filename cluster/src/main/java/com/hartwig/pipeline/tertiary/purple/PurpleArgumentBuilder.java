package com.hartwig.pipeline.tertiary.purple;

import static com.hartwig.pipeline.metadata.InputMode.REFERENCE_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_REFERENCE;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public final class PurpleArgumentBuilder
{
    private static final String CIRCOS_PATH = VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos";

    public static List<String> buildArguments(final Purple parent, final SomaticRunMetadata metadata, final ResourceFiles resourceFiles) {
        List<String> arguments = Lists.newArrayList();

        if(metadata.mode() == TUMOR_REFERENCE || metadata.mode() == TUMOR_ONLY) {
            arguments.add(String.format("-tumor %s", metadata.tumor().sampleName()));
            arguments.add(String.format("-somatic_vcf %s", parent.somaticVariantsDownload.getLocalTargetPath()));
            arguments.add(String.format("-structural_vcf %s", parent.structuralVariantsDownload.getLocalTargetPath()));
            arguments.add(String.format("-sv_recovery_vcf %s", parent.svRecoveryVariantsDownload.getLocalTargetPath()));
            arguments.add(String.format("-somatic_hotspots %s", resourceFiles.sageSomaticHotspots()));
        }

        if(metadata.mode() == TUMOR_REFERENCE || metadata.mode() == REFERENCE_ONLY) {
            arguments.add(String.format("-reference %s", metadata.reference().sampleName()));
            arguments.add(String.format("-germline_vcf %s", parent.germlineVariantsDownload.getLocalTargetPath()));
            arguments.add(String.format("-germline_hotspots %s", resourceFiles.sageGermlineHotspots()));
            arguments.add(String.format("-germline_del_freq_file %s", resourceFiles.purpleCohortGermlineDeletions()));
        }

        arguments.add(String.format("-amber %s", parent.amberOutputDownload.getLocalTargetPath()));
        arguments.add(String.format("-cobalt %s", parent.cobaltOutputDownload.getLocalTargetPath()));

        addCommonArguments(arguments, resourceFiles);

        if(resourceFiles.targetRegionsEnabled()) {
            arguments.add(String.format("-target_regions_bed %s", resourceFiles.targetRegionsBed()));
            // arguments.add(String.format("-target_regions_ratios %s", resourceFiles.sageSomaticHotspots())); // may not be used
            arguments.add("-min_diploid_tumor_ratio_count 0");
            arguments.add("-min_diploid_tumor_ratio_count_centromere 0");
        }

        // no plots for germline-only mode
        if(metadata.mode() == REFERENCE_ONLY) {
            arguments.add("-no_charts");
        }
        else {
            arguments.add(String.format("-circos %s", CIRCOS_PATH));
        }

        return arguments;
    }

    private static void addCommonArguments(final List<String> arguments, final ResourceFiles resourceFiles) {

        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add("-run_drivers");
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-gc_profile %s", resourceFiles.gcProfileFile()));

        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));
    }

    public static void addShallowArguments(final List<String> arguments) {
        arguments.add("-highly_diploid_percentage 0.88");
        arguments.add("-somatic_min_purity_spread 0.1");
    }
}

