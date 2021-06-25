package com.hartwig.pipeline.tertiary.protect;

import java.util.List;

import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

public class ProtectCommand extends JavaJarCommand {

    public ProtectCommand(final String tumorSample, final String referenceSample, final List<String> primaryTumorDoids,
            final String outputDir, final String actionabilityDir, final RefGenomeVersion refGenomeVersion, final String doidJsonPath,
            final String purplePurityPath, final String purpleQCFilePath, final String purpleGeneCopyNumberTsvPath,
            final String purpleSomaticDriverCatalogPath, final String purpleGermlineDriverCatalogPath,
            final String purpleSomaticVariantsPath, final String purpleGermlineVariantsPath, final String linxFusionTsvPath,
            final String linxBreakendTsvPath, final String linxDriverCatalogPath, final String annotatedVirusTsvPath,
            final String chordPredictionPath) {
        super("protect",
                Versions.PROTECT,
                "protect.jar",
                "8G",
                List.of("-tumor_sample_id",
                        tumorSample,
                        "-reference_sample_id",
                        referenceSample,
                        "-primary_tumor_doids",
                        primaryTumorDoids.isEmpty() ? "\"\"" : "\"" + String.join(";", primaryTumorDoids) + "\"",
                        "-output_dir",
                        outputDir,
                        "-serve_actionability_dir",
                        actionabilityDir,
                        "-ref_genome_version",
                        refGenomeVersion.protect(),
                        "-doid_json",
                        doidJsonPath,
                        "-purple_purity_tsv",
                        purplePurityPath,
                        "-purple_qc_file",
                        purpleQCFilePath,
                        "-purple_gene_copy_number_tsv",
                        purpleGeneCopyNumberTsvPath,
                        "-purple_somatic_driver_catalog_tsv",
                        purpleSomaticDriverCatalogPath,
                        "-purple_germline_driver_catalog_tsv",
                        purpleGermlineDriverCatalogPath,
                        "-purple_somatic_variant_vcf",
                        purpleSomaticVariantsPath,
                        "-purple_germline_variant_vcf",
                        purpleGermlineVariantsPath,
                        "-linx_fusion_tsv",
                        linxFusionTsvPath,
                        "-linx_breakend_tsv",
                        linxBreakendTsvPath,
                        "-linx_driver_catalog_tsv",
                        linxDriverCatalogPath,
                        "-annotated_virus_tsv",
                        annotatedVirusTsvPath,
                        "-chord_prediction_txt",
                        chordPredictionPath));
    }
}
