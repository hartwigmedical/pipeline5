package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.resource.RefGenomeVersion.HG37;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

class LinxCommand extends JavaJarCommand {
    LinxCommand(final String sample, final String svVcf, final String purpleDir, final String referenceGenome,
            final RefGenomeVersion refGenomeVersion, final String outputDir, final String fragileSiteFile, final String lineElementFile,
            final String replicationsOriginsFile, final String viralHostsFile, final String geneTranscriptsDirectory,
            final String knownFusionData) {
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
                        "-ref_genome_version",
                        asString(refGenomeVersion),
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
                        "-known_fusion_file",
                        knownFusionData,
                        "-chaining_sv_limit",
                        "0",
                        "-check_drivers",
                        "-write_vis_data").build());
    }

    public static String asString(RefGenomeVersion version) {
        return version == HG37 ? "37" : "38";
    }
}
