package com.hartwig.pipeline.tertiary.protect;

import java.util.List;

import com.hartwig.pipeline.execution.vm.JavaJarCommand;

public class ProtectCommand extends JavaJarCommand {

    public ProtectCommand(final String tumorSample, final List<String> primaryTumorDoids, final String outputDir,
            final String actionabilityDir, final String doidJsonPath, final String germlineReportingTsvPath, final String purplePurityPath,
            final String purpleQCFilePath, final String purpleDriverCatalogPath, final String purpleSomaticVariantsPath,
            final String bachelorTsvPath, final String linxFusionTsvPath, final String linxBreakendTsvPath, final String linxDriversTsvPath,
            final String linxViralInsertionsTsvPath, final String chordPredictionPath) {
        super("protect",
                "1.1",
                "protect.jar",
                "8G",
                List.of("-tumor_sample_id",
                        tumorSample,
                        "-primary_tumor_doids",
                        primaryTumorDoids.isEmpty() ? "\"\"" : String.join(";", primaryTumorDoids),
                        "-output_dir",
                        outputDir,
                        "-serve_actionability_dir",
                        actionabilityDir,
                        "-doid_json",
                        doidJsonPath,
                        "-germline_reporting_tsv",
                        germlineReportingTsvPath,
                        "-purple_purity_tsv",
                        purplePurityPath,
                        "-purple_qc_file",
                        purpleQCFilePath,
                        "-purple_driver_catalog_tsv",
                        purpleDriverCatalogPath,
                        "-purple_somatic_variant_vcf",
                        purpleSomaticVariantsPath,
                        "-bachelor_tsv",
                        bachelorTsvPath,
                        "-linx_fusion_tsv",
                        linxFusionTsvPath,
                        "-linx_breakend_tsv",
                        linxBreakendTsvPath,
                        "-linx_viral_insertion_tsv",
                        linxViralInsertionsTsvPath,
                        "-linx_drivers_tsv",
                        linxDriversTsvPath,
                        "-chord_prediction_txt",
                        chordPredictionPath,
                        "-log_debug"));
    }
}
