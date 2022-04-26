package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestInputs.REFERENCE_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.TUMOR_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class GridssTest extends StageTest<GridssOutput, SomaticRunMetadata> {
    private static final String TUMOR_GRIDSS_UNFILTERED_VCF_GZ = "tumor.gridss.unfiltered.vcf.gz";
    private static final String GRIDSS = "gridss/";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected Stage<GridssOutput, SomaticRunMetadata> createVictim() {
        return new Gridss(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(inputDownload(REFERENCE_BUCKET, "reference.bam"),
                inputDownload(REFERENCE_BUCKET, "reference.bam.bai"),
                inputDownload(TUMOR_BUCKET, "tumor.bam"),
                inputDownload(TUMOR_BUCKET, "tumor.bam.bai"));
    }

    private String inputDownload(final String bucket, final String basename) {
        return input(format("%s/aligner/results/%s", bucket, basename), basename);
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return TestInputs.SOMATIC_BUCKET;
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "export PATH=\"${PATH}:/opt/tools/samtools/1.14\"",
                "/opt/tools/gridss/2.13.2/gridss --output /data/output/tumor.gridss.driver.vcf.gz --assembly /data/output/tumor"
                        + ".assembly.bam --workingdir /data/output --reference /opt/resources/reference_genome/37/Homo_sapiens.GRCh37"
                        + ".GATK.illumina.fasta --jar /opt/tools/gridss/2.13.2/gridss.jar --blacklist "
                        + "/opt/resources/gridss_repeatmasker_db/37/ENCFF001TDO.37.bed --configuration /opt/resources/gridss_config/gridss.properties --labels reference,tumor --jvmheap 31G --externalaligner /data/input/reference.bam /data/input/tumor.bam",
                "/opt/tools/gridss/2.13.2/gridss_annotate_vcf_repeatmasker --output /data/output/tumor.gridss.repeatmasker.vcf.gz "
                        + "--jar /opt/tools/gridss/2.13.2/gridss.jar -w /data/output --rm /opt/tools/repeatmasker/4.1.1/RepeatMasker "
                        + "/data/output/tumor.gridss.driver.vcf.gz",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk"
                        + ".use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp "
                        + "/opt/tools/gridss/2.13.2/gridss.jar gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/tumor.gridss.repeatmasker.vcf.gz OUTPUT=/data/output/tumor.gridss.unfiltered.vcf.gz ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected void validateOutput(final GridssOutput output) {
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
    protected void validatePersistedOutput(final GridssOutput output) {
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/" + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_GRIDSS, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final GridssOutput output) {
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }
}