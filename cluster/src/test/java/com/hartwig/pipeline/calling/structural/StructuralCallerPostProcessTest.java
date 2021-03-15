package com.hartwig.pipeline.calling.structural;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class StructuralCallerPostProcessTest extends StageTest<StructuralCallerPostProcessOutput, SomaticRunMetadata> {
    private static final String TUMOR_GRIPSS_SOMATIC_VCF_GZ = "tumor.gripss.somatic.vcf.gz";
    private static final String TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ = "tumor.gripss.somatic.filtered.vcf.gz";
    private static final String GRIPSS = "gripss/";

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
        return new StructuralCallerPostProcess(TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.structuralCallerOutput(), persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/gridss/results/tumor.gridss.unfiltered.vcf.gz",
                "tumor.gridss.unfiltered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gridss/results/tumor.gridss.unfiltered.vcf.gz.tbi",
                        "tumor.gridss.unfiltered.vcf.gz.tbi"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return TestInputs.SOMATIC_BUCKET;
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx24G -cp /opt/tools/gripss/1.11/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt -ref_genome "
                        + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-breakpoint_hotspot /opt/resources/knowledgebases/37/known_fusions.bedpe "
                        + "-breakend_pon /opt/resources/gridss_pon/37/gridss_pon_single_breakend.hg19.bed "
                        + "-breakpoint_pon /opt/resources/gridss_pon/37/gridss_pon_breakpoint.hg19.bedpe "
                        + "-reference reference -tumor tumor "
                        + "-input_vcf /data/input/tumor.gridss.unfiltered.vcf.gz -output_vcf /data/output/" + TUMOR_GRIPSS_SOMATIC_VCF_GZ
                        + " -paired_normal_tumor_ordinals",
                "java -Xmx24G -cp /opt/tools/gripss/1.11/gripss.jar com.hartwig.hmftools.gripss.GripssHardFilterApplicationKt -input_vcf /data/output/"
                        + TUMOR_GRIPSS_SOMATIC_VCF_GZ + " -output_vcf /data/output/" + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ);
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
    public void addsLogs() {
        // not supported currently
    }

    @Override
    protected void validatePersistedOutput(final StructuralCallerPostProcessOutput output) {
        assertThat(output.filteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gripss/" + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ));
        assertThat(output.filteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gripss/" + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ + ".tbi"));
        assertThat(output.fullVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/gripss/" + TUMOR_GRIPSS_SOMATIC_VCF_GZ));
        assertThat(output.fullVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gripss/" + TUMOR_GRIPSS_SOMATIC_VCF_GZ + ".tbi"));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_GRIPSS_RECOVERY, GRIPSS + TUMOR_GRIPSS_SOMATIC_VCF_GZ);
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_GRIPSS, GRIPSS + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final StructuralCallerPostProcessOutput output) {
        assertThat(output.filteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIPSS + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ));
        assertThat(output.filteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                GRIPSS + TUMOR_GRIPSS_SOMATIC_FILTERED_VCF_GZ + ".tbi"));
        assertThat(output.fullVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIPSS + TUMOR_GRIPSS_SOMATIC_VCF_GZ));
        assertThat(output.fullVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIPSS + TUMOR_GRIPSS_SOMATIC_VCF_GZ + ".tbi"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }
}