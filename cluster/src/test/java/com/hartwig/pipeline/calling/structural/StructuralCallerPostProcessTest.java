package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class StructuralCallerPostProcessTest extends StageTest<StructuralCallerPostProcessOutput, SomaticRunMetadata> {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runStructuralCaller(false).build();
    }

    @Override
    protected Stage<StructuralCallerPostProcessOutput, SomaticRunMetadata> createVictim() {
        return new StructuralCallerPostProcess(TestInputs.HG19_RESOURCE_FILES, TestInputs.structuralCallerOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/gridss/results/tumor.gridss.unfiltered.vcf.gz",
                "tumor.gridss.unfiltered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gridss/results/tumor.gridss.unfiltered.vcf.gz.tbi",
                        "tumor.gridss.unfiltered.vcf.gz.tbi"));
    }

    private String inputDownload(String bucket, String basename) {
        return input(format("%s/aligner/results/%s", bucket, basename), basename);
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return TestInputs.SOMATIC_BUCKET;
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx24G -cp /opt/tools/gripss/1.7/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt -ref_genome /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -breakpoint_hotspot /opt/resources/knowledgebases/hg19/KnownFusionPairs.hg19.bedpe -breakend_pon /opt/resources/gridss_pon/hg19/gridss_pon_single_breakend.hg19.bed -breakpoint_pon /opt/resources/gridss_pon/hg19/gridss_pon_breakpoint.hg19.bedpe -input_vcf /data/input/tumor.gridss.unfiltered.vcf.gz -output_vcf /data/output/tumor.gridss.somatic.vcf.gz",
                "java -Xmx24G -cp /opt/tools/gripss/1.7/gripss.jar com.hartwig.hmftools.gripss.GripssHardFilterApplicationKt -input_vcf /data/output/tumor.gridss.somatic.vcf.gz -output_vcf /data/output/tumor.gridss.somatic.filtered.vcf.gz");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected void validateOutput(final StructuralCallerPostProcessOutput output) {
        // no further validation yet
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported currently
    }

    @Override
    protected void validatePersistedOutput(final StructuralCallerPostProcessOutput output) {
        assertThat(output.filteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "run/gripss/tumor.gridss.somatic.filtered.vcf.gz"));
        assertThat(output.filteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "run/gripss/tumor.gridss.somatic.filtered.vcf.gz.tbi"));
        assertThat(output.fullVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "run/gripss/tumor.gridss.somatic.vcf.gz"));
        assertThat(output.fullVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "run/gripss/tumor.gridss.somatic.vcf.gz.tbi"));
    }
}