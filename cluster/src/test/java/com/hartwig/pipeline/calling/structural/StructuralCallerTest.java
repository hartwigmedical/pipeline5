package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestInputs.REFERENCE_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.TUMOR_BUCKET;

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

public class StructuralCallerTest extends StageTest<StructuralCallerOutput, SomaticRunMetadata> {
    private static final String TUMOR_GRIDSS_UNFILTERED_VCF_GZ = "tumor.gridss.unfiltered.vcf.gz";
    private static final String GRIDSS = "gridss/";

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
    protected Stage<StructuralCallerOutput, SomaticRunMetadata> createVictim() {
        return new StructuralCaller(TestInputs.defaultPair(), TestInputs.HG19_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(inputDownload(REFERENCE_BUCKET, "reference.bam"),
                inputDownload(REFERENCE_BUCKET, "reference.bam.bai"),
                inputDownload(TUMOR_BUCKET, "tumor.bam"),
                inputDownload(TUMOR_BUCKET, "tumor.bam.bai"));
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
        return ImmutableList.of("export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "export PATH=\"${PATH}:/opt/tools/samtools/1.10\"",
                "/opt/tools/gridss/2.9.3/gridss.sh -o /data/output/tumor.gridss.driver.vcf.gz -a /data/output/tumor.assembly.bam -w /data/output -r /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -j /opt/tools/gridss/2.9.3/gridss.jar -b /opt/resources/gridss_repeatmasker_db/hg19/ENCFF001TDO.bed -c /opt/resources/gridss_config/gridss.properties --repeatmaskerbed /opt/resources/gridss_repeatmasker_db/hg19/hg19.fa.out.bed --jvmheap 31G /data/input/reference.bam /data/input/tumor.bam",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.gridss.driver.vcf.gz -p vcf",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.9.3/gridss.jar gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/tumor.gridss.driver.vcf.gz OUTPUT=/data/output/"
                        + TUMOR_GRIDSS_UNFILTERED_VCF_GZ + " ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected void validateOutput(final StructuralCallerOutput output) {
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
    protected void validatePersistedOutput(final StructuralCallerOutput output) {
        assertThat(output.unfilteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/" + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
        assertThat(output.unfilteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/" + TUMOR_GRIDSS_UNFILTERED_VCF_GZ + ".tbi"));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final StructuralCallerOutput output) {
        assertThat(output.unfilteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
        assertThat(output.unfilteredVcfIndex()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ + ".tbi"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }
}