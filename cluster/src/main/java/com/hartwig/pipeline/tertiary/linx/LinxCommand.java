package com.hartwig.pipeline.tertiary.linx;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

class LinxCommand extends JavaJarCommand {
    LinxCommand(final String sample, final String svVcf, final String purpleDir, final RefGenomeVersion refGenomeVersion,
            final String outputDir, final String fragileSiteFile, final String lineElementFile,
            final String viralHostsFile, final String geneTranscriptsDirectory, final String knownFusionData,
            final String driverGenePanel) {
        super("linx",
                Versions.LINX,
                "linx.jar",
                "8G",
                ImmutableList.<String>builder().add("-sample",
                        sample,
                        "-sv_vcf",
                        svVcf,
                        "-purple_dir",
                        purpleDir,
                        "-ref_genome_version",
                        refGenomeVersion.linx(),
                        "-output_dir",
                        outputDir,
                        "-fragile_site_file",
                        fragileSiteFile,
                        "-line_element_file",
                        lineElementFile,
                        "-viral_hosts_file",
                        viralHostsFile,
                        "-ensembl_data_dir",
                        geneTranscriptsDirectory,
                        "-check_fusions",
                        "-known_fusion_file",
                        knownFusionData,
                        "-check_drivers",
                        "-driver_gene_panel",
                        driverGenePanel,
                        "-chaining_sv_limit",
                        "0",
                        "-write_vis_data").build());
    }
}
