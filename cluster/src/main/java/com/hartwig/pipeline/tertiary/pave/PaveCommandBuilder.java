package com.hartwig.pipeline.tertiary.pave;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class PaveCommandBuilder
{
    private final ResourceFiles resourceFiles;
    private final String tumorSampleName;
    private final String vcfFile;

    public PaveCommandBuilder(final ResourceFiles resourceFiles, final String tumorSample, final String vcfFile) {
        this.resourceFiles = resourceFiles;
        this.tumorSampleName = tumorSample;
        this.vcfFile = vcfFile;
    }

    public BashCommand build() {

        final List<String> arguments = Lists.newArrayList(
                "-sample",
                tumorSampleName,
                "-vcf_file",
                vcfFile,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-ensembl_data_dir",
                resourceFiles.ensemblDataCache(),
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-ref_genome_version",
                resourceFiles.version().toString(),
                "-driver_gene_panel",
                resourceFiles.driverGenePanel());

        return new JavaJarCommand("pave", Versions.PAVE, "pave.jar", "8G", arguments);
    }
}

