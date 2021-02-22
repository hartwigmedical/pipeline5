package com.hartwig.pipeline.tertiary.protect;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class ProtectTest extends TertiaryStageTest<ProtectOutput> {

    @Override
    protected Stage<ProtectOutput, SomaticRunMetadata> createVictim() {
        return new Protect(TestInputs.purpleOutput(),
                TestInputs.bachelorOutput(),
                TestInputs.linxOutput(),
                TestInputs.chordOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.somatic.tsv", "tumor.driver.catalog.somatic.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/bachelor/tumor.reportable_germline_variant.tsv",
                        "tumor.reportable_germline_variant.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.drivers.tsv", "tumor.linx.drivers.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.viral_inserts.tsv", "tumor.linx.viral_inserts.tsv"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx8G -jar /opt/tools/protect/1.1/protect.jar -tumor_sample_id tumor -primary_tumor_doids 01;02 "
                        + "-output_dir /data/output -serve_actionability_dir /opt/resources/serve/37/ -doid_json "
                        + "/opt/resources/disease_ontology/201015_doid.json -germline_reporting_tsv "
                        + "/opt/resources/germline_reporting/germline_reporting.tsv -purple_purity_tsv /data/input/tumor.purple.purity.tsv "
                        + "-purple_qc_file /data/input/tumor.purple.qc -purple_driver_catalog_tsv /data/input/tumor.driver.catalog.somatic.tsv "
                        + "-purple_somatic_variant_vcf /data/input/tumor.purple.somatic.vcf.gz -bachelor_tsv "
                        + "/data/input/tumor.reportable_germline_variant.tsv -linx_fusion_tsv /data/input/tumor.linx.fusion.tsv -linx_breakend_tsv "
                        + "/data/input/tumor.linx.breakend.tsv -linx_viral_insertion_tsv /data/input/tumor.linx.viral_inserts.tsv -linx_drivers_tsv "
                        + "/data/input/tumor.linx.drivers.tsv -chord_prediction_txt /data/input/tumor_chord_prediction.txt -log_debug");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final ProtectOutput output) {
        // no additional validation
    }
}