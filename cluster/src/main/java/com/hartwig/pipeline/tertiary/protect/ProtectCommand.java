package com.hartwig.pipeline.tertiary.protect;

import java.util.List;

import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

public class ProtectCommand extends JavaJarCommand {

    public ProtectCommand(final String tumorSample, final List<String> primaryTumorDoids, final String outputDir,
            final String actionabilityDir, final String doidJsonPath, final String purplePurityPath, final String purpleQCFilePath,
            final String purpleDriverCatalogSomaticPath, final String purpleDriverCatalogGermlinePath,
            final String purpleSomaticVariantsPath, final String purpleGermlineVariantsPath, final String linxFusionTsvPath,
            final String linxBreakendTsvPath, final String linxDriversTsvPath, final String chordPredictionPath) {
        super("protect",
                Versions.PROTECT,
                "protect.jar",
                "8G",
                List.of("-tumor_sample_id",
                        tumorSample,
                        "-primary_tumor_doids",
                        primaryTumorDoids.isEmpty() ? "\"\"" : "\"" + String.join(";", primaryTumorDoids) + "\"",
                        "-output_dir",
                        outputDir,
                        "-serve_actionability_dir",
                        actionabilityDir,
                        "-doid_json",
                        doidJsonPath,
                        "-purple_purity_tsv",
                        purplePurityPath,
                        "-purple_qc_file",
                        purpleQCFilePath,
                        "-purple_somatic_driver_catalog_tsv",
                        purpleDriverCatalogSomaticPath,
                        "-purple_germline_driver_catalog_tsv",
                        purpleDriverCatalogGermlinePath,
                        "-purple_somatic_variant_vcf",
                        purpleSomaticVariantsPath,
                        "-purple_germline_variant_vcf",
                        purpleGermlineVariantsPath,
                        "-linx_fusion_tsv",
                        linxFusionTsvPath,
                        "-linx_breakend_tsv",
                        linxBreakendTsvPath,
                        "-linx_driver_catalog_tsv",
                        linxDriversTsvPath,
                        "-chord_prediction_txt",
                        chordPredictionPath,
                        "-log_debug"));
    }
}
