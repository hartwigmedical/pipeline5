package com.hartwig.pipeline.tertiary.bachelor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class BachelorTest extends TertiaryStageTest<BachelorOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<BachelorOutput, SomaticRunMetadata> createVictim() {
        return new Bachelor(TestInputs.HG37_RESOURCE_FILES, TestInputs.purpleOutput(), TestInputs.tumorAlignmentOutput(), TestInputs.germlineCallerOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/results/", "results"),
                input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/germline_caller/reference.germline.vcf.gz", "reference.germline.vcf.gz"),
                input("run-reference-test/germline_caller/reference.germline.vcf.gz.tbi", "reference.germline.vcf.gz.tbi"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar /opt/tools/bachelor/1.9/bachelor.jar -sample tumor -germline_vcf "
                + "/data/input/reference.germline.vcf.gz -tumor_bam_file /data/input/tumor.bam -purple_data_dir /data/input/results "
                + "-xml_config /opt/resources/bachelor_config/bachelor_hmf.xml -ext_filter_file "
                + "/opt/resources/bachelor_config/bachelor_clinvar_filters.csv -ref_genome "
                + "/opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -output_dir /data/output -log_debug");
    }

    @Test
    public void doesntRunWhenShallowEnabled() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().shallow(true).runTertiary(true).build())).isFalse();
    }

    @Test
    public void doesntRunWhenGermlineDisabled() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().runGermlineCaller(false).runTertiary(true).build())).isFalse();
    }

    @Override
    protected void validateOutput(final BachelorOutput output) {
        // no additional validation
    }
}