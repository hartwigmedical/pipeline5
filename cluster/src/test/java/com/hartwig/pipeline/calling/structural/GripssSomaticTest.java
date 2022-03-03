package com.hartwig.pipeline.calling.structural;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.gripss.GripssSomatic;
import com.hartwig.pipeline.calling.structural.gripss.GripssSomaticOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class GripssSomaticTest extends StageTest<GripssSomaticOutput, SomaticRunMetadata> {

    private static final String TUMOR_GRIPSS_VCF_GZ = "tumor.gripss.somatic.vcf.gz";
    private static final String TUMOR_GRIPSS_FILTERED_VCF_GZ = "tumor.gripss.filtered.somatic.vcf.gz";
    private static final String GRIPSS = "gripss_somatic/";

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
    protected Stage<GripssSomaticOutput, SomaticRunMetadata> createVictim() {
        return new GripssSomatic(TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.structuralCallerOutput(), persistedDataset);
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
        return ImmutableList.of("java -Xmx16G -jar /opt/tools/gripss/2.0/gripss.jar -ref_genome "
                + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-known_hotspot_file /opt/resources/fusions/37/known_fusions.37.bedpe "
                + "-pon_sgl_file /opt/resources/gridss_pon/37/gridss_pon_single_breakend.37.bed "
                + "-pon_sv_file /opt/resources/gridss_pon/37/gridss_pon_breakpoint.37.bedpe " + "-reference reference -sample tumor "
                + "-vcf /data/input/tumor.gridss.unfiltered.vcf.gz -output_dir /data/output -output_id somatic");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected void validateOutput(final GripssSomaticOutput output) {
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
    protected void validatePersistedOutput(final GripssSomaticOutput output) {
        String outputDir = "set/" + GRIPSS;
        assertThat(output.filteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, outputDir + TUMOR_GRIPSS_FILTERED_VCF_GZ));
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, outputDir + TUMOR_GRIPSS_VCF_GZ));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY, GRIPSS + TUMOR_GRIPSS_VCF_GZ);
        persistedDataset.addPath(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS, GRIPSS + TUMOR_GRIPSS_FILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final GripssSomaticOutput output) {
        assertThat(output.filteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIPSS + TUMOR_GRIPSS_FILTERED_VCF_GZ));
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIPSS + TUMOR_GRIPSS_VCF_GZ));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }
}