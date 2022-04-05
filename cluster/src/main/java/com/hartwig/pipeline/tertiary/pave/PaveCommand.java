package com.hartwig.pipeline.tertiary.pave;

import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

public class PaveCommand extends JavaJarCommand {

    public PaveCommand(final ResourceFiles resourceFiles, final String tumorSampleName, final String vcfFile) {
        super("pave", Versions.PAVE, "pave.jar", "8G", arguments(tumorSampleName, vcfFile, resourceFiles));
    }

    @NotNull
    private static List<String> arguments(final String tumorSampleName, final String vcfFile, final ResourceFiles resourceFiles) {
        return List.of("-sample",
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
    }
}