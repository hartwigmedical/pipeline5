package com.hartwig.pipeline.tertiary.cuppa;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CuppaTest extends TertiaryStageTest<CuppaOutput> {

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).build())).isTrue();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return Collections.emptyList();
    }

    @Override
    protected Stage<CuppaOutput, SomaticRunMetadata> createVictim() {
        return new Cuppa(purpleOutput(), linxOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx4G -jar /opt/tools/cuppa/1.4/cuppa.jar -categories DNA -ref_data_dir /opt/resources/cuppa "
                        + "-sample_data tumor -sample_data_dir /data/input -sample_sv_file /data/input/tumor.purple.sv.vcf.gz "
                        + "-sample_somatic_vcf /data/input/tumor.purple.somatic.vcf.gz -log_debug -output_dir /data/output",
                "/opt/tools/cuppa-chart/1.0_venv/bin/python /opt/tools/cuppa-chart/1.0/src/cuppa-chart.py -sample tumor "
                        + "-sample_data /data/output/tumor.cup.data.csv -output_dir /data/output");
    }

    @Override
    protected void validateOutput(final CuppaOutput output) {
        // no additional validation
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.driver.catalog.tsv", "tumor.linx.driver.catalog.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.viral_inserts.tsv", "tumor.linx.viral_inserts.tsv"));
    }
}