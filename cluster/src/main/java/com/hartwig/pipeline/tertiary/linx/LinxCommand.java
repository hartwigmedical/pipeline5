package com.hartwig.pipeline.tertiary.linx;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

class LinxCommand extends JavaJarCommand {
    LinxCommand(final String sample, final String svVcf, final String purpleDir, final String referenceGenome, final String outputDir,
            final String fragileSiteFile, final String lineElementFile, final String replicationsOriginsFile, final String viralHostsFile,
            final String geneTranscriptsDirectory, final String fusionsPairsCsv, final String promiscuousFiveCsv,
            final String promiscuousThreeCsv) {
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
                        "-ref_genome",
                        referenceGenome,
                        "-output_dir",
                        outputDir,
                        "-fragile_site_file",
                        fragileSiteFile,
                        "-line_element_file",
                        lineElementFile,
                        "-replication_origins_file",
                        replicationsOriginsFile,
                        "-viral_hosts_file",
                        viralHostsFile,
                        "-gene_transcripts_dir",
                        geneTranscriptsDirectory,
                        "-check_fusions",
                        "-fusion_pairs_csv",
                        fusionsPairsCsv,
                        "-promiscuous_five_csv",
                        promiscuousFiveCsv,
                        "-promiscuous_three_csv",
                        promiscuousThreeCsv,
                        "-chaining_sv_limit",
                        "0",
                        "-check_drivers",
                        "-write_vis_data").build());
    }
}
