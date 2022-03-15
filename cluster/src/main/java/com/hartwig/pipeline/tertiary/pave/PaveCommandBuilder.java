package com.hartwig.pipeline.tertiary.pave;

import java.util.List;
import java.util.StringJoiner;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;

public class PaveCommandBuilder
{
    private final ResourceFiles resourceFiles;

    private boolean somaticMode;
    private boolean germlineMode;
    private boolean tumorOnly;

    public PaveCommandBuilder(ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
        this.somaticMode = true;
        this.germlineMode = false;
        this.tumorOnly = false;
    }

    public PaveCommandBuilder germlineMode() {
        germlineMode = true;
        somaticMode = false;
        return this;
    }

    public PaveCommandBuilder tumorOnly() {
        tumorOnly = true;
        return this;
    }

    public List<BashCommand> build(final String tumorSampleName, final String vcfFile) {

        List<BashCommand> result = Lists.newArrayList();

        final StringJoiner arguments = new StringJoiner(" ");

        arguments.add(String.format("-sample %s", tumorSampleName));
        arguments.add(String.format("-vcf_file %s", vcfFile));

        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add("-read_pass_only");

        if(tumorOnly) {
            arguments.add("-write_pass_only");
        }

        if(somaticMode) {
            String ponFilters = resourceFiles.version() == RefGenomeVersion.V37 ?
                    "HOTSPOT:10:5;PANEL:6:5;UNKNOWN:6:0" : "HOTSPOT:5:5;PANEL:2:5;UNKNOWN:2:0";

            arguments.add(String.format("-pon_file %s/%s", resourceFiles.germlinePon()));
            arguments.add(String.format("-pon_artefact_file %s/%s", resourceFiles.somaticPonArtefacts()));
            arguments.add(String.format("-pon_filters \"%s\"", ponFilters));

            if(resourceFiles.version() == RefGenomeVersion.V38)
            {
                arguments.add(String.format("-gnomad_freq_dir %s", resourceFiles.gnomadPonCache()));
                arguments.add(String.format("-gnomad_load_chr_on_demand", resourceFiles.gnomadPonCache()));
            }
        }

        if(germlineMode) {

        }

        return result;
    }



}
