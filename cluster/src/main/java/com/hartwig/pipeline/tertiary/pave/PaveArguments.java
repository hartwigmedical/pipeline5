package com.hartwig.pipeline.tertiary.pave;

import static java.lang.String.format;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;

public class PaveArguments {
    private static final String PON_FILTERS_V37 = "HOTSPOT:10:5;PANEL:6:5;UNKNOWN:6:0";
    private static final String PON_FILTERS_V38 = "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

    public static List<String> somatic(final ResourceFiles resourceFiles, final String tumorSampleName, final String inputVcf,
            final String outputVcf) {

        List<String> arguments = Lists.newArrayList();

        addCommonArguments(arguments, resourceFiles, tumorSampleName, inputVcf, outputVcf);

        arguments.add(format("-pon_file %s", resourceFiles.germlinePon()));

        String ponFilters = resourceFiles.version() == RefGenomeVersion.V37 ? PON_FILTERS_V37 : PON_FILTERS_V38;
        arguments.add(format("-pon_filters \"%s\"", ponFilters));

        return arguments;
    }

    public static List<String> germline(final ResourceFiles resourceFiles, final String tumorSampleName, final String inputVcf,
            final String outputVcf) {

        List<String> arguments = Lists.newArrayList();

        addCommonArguments(arguments, resourceFiles, tumorSampleName, inputVcf, outputVcf);
        arguments.add(format("-clinvar_vcf %s", resourceFiles.clinvarVcf()));
        arguments.add(format("-blacklist_bed %s", resourceFiles.germlineBlacklistBed()));
        arguments.add(format("-blacklist_vcf %s", resourceFiles.germlineBlacklistVcf()));
        arguments.add("-gnomad_pon_filter -1"); // disable filtering in germline mode while still annotating

        return arguments;
    }

    private static void addCommonArguments(final List<String> arguments, final ResourceFiles resourceFiles, final String tumorSampleName,
            final String inputVcf, final String outputVcf) {

        arguments.add(format("-sample %s", tumorSampleName));
        arguments.add(format("-vcf_file %s", inputVcf));
        arguments.add(format("-output_vcf_file %s/%s", VmDirectories.OUTPUT, outputVcf));

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add(format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(format("-mappability_bed %s", resourceFiles.mappabilityBed()));

        if (resourceFiles.version() == RefGenomeVersion.V38) {
            arguments.add(format("-gnomad_freq_dir %s", resourceFiles.gnomadPonCache()));
        } else {
            arguments.add(format("-gnomad_freq_file %s", resourceFiles.gnomadPonCache()));
        }

        arguments.add("-read_pass_only");
        arguments.add(format("-threads %s", Bash.allCpus()));
    }

    public static List<String> addTargetRegionsArguments(final ResourceFiles resourceFiles) {

        return List.of(
                format("-pon_artefact_file %s", resourceFiles.targetRegionsPonArtefacts()),
                "-force_pathogenic_pass", // annotate with Clinvar but don't filter
                format("-clinvar_vcf %s", resourceFiles.clinvarVcf()),
                format("-blacklist_bed %s", resourceFiles.germlineBlacklistBed()),
                format("-blacklist_vcf %s", resourceFiles.germlineBlacklistVcf()));
    }
}