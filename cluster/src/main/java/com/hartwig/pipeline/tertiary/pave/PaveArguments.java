package com.hartwig.pipeline.tertiary.pave;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;

public class PaveArguments {
    private static final String PON_FILTERS_V37 = "HOTSPOT:10:5;PANEL:6:5;UNKNOWN:6:0";
    private static final String PON_FILTERS_V38 = "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

    public static List<String> somatic(
            final ResourceFiles resourceFiles, final String tumorSampleName, final String inputVcf, final String outputVcf) {

        List<String> arguments = Lists.newArrayList();

        addCommonArguments(arguments, resourceFiles, tumorSampleName, inputVcf, outputVcf);

        arguments.add(String.format("-pon_file %s", resourceFiles.germlinePon()));
        arguments.add(String.format("-pon_artefact_file %s", resourceFiles.somaticPonArtefacts()));

        String ponFilters = resourceFiles.version() == RefGenomeVersion.V37 ? PON_FILTERS_V37 : PON_FILTERS_V38;
        arguments.add(String.format("-pon_filters \"%s\"", ponFilters));

        return arguments;
    }

    public static List<String> germline(
            final ResourceFiles resourceFiles, final String tumorSampleName, final String inputVcf, final String outputVcf) {

        List<String> arguments = Lists.newArrayList();

        addCommonArguments(arguments, resourceFiles, tumorSampleName, inputVcf, outputVcf);
        arguments.add(String.format("-clinvar_vcf %s", resourceFiles.clinvarVcf()));
        arguments.add(String.format("-blacklist_bed %s", resourceFiles.germlineBlacklistBed()));
        arguments.add(String.format("-blacklist_vcf %s", resourceFiles.germlineBlacklistVcf()));
        arguments.add("-gnomad_pon_filter -1"); // disable filtering in germline mode while still annotating

        return arguments;
    }

    private static void addCommonArguments(final List<String> arguments, final ResourceFiles resourceFiles, final String tumorSampleName,
            final String inputVcf, final String outputVcf) {

        arguments.add(String.format("-sample %s", tumorSampleName));
        arguments.add(String.format("-vcf_file %s", inputVcf));
        arguments.add(String.format("-output_vcf_file %s/%s", VmDirectories.OUTPUT, outputVcf));

        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-mappability_bed %s", resourceFiles.mappabilityBed()));

        if (resourceFiles.version() == RefGenomeVersion.V38) {
            arguments.add(String.format("-gnomad_freq_dir %s", resourceFiles.gnomadPonCache()));
            arguments.add("-gnomad_load_chr_on_demand");
        }
        else
        {
            arguments.add(String.format("-gnomad_freq_file %s", resourceFiles.gnomadPonCache()));
        }

        arguments.add("-read_pass_only");
    }
}
