package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestInputs.REFERENCE_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.TUMOR_BUCKET;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class StructuralCallerTest extends StageTest<StructuralCallerOutput, SomaticRunMetadata> {
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
        return new StructuralCaller(TestInputs.defaultPair(), TestInputs.HG19_RESOURCE_FILES);
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
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.9.3/gridss.jar gridss.AnnotateInsertedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/tumor.gridss.driver.vcf.gz OUTPUT=/data/output/tumor.gridss.unfiltered.vcf.gz ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)",
                "java -Xmx24G -cp /opt/tools/gripss/1.7/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt -ref_genome /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -breakpoint_hotspot /opt/resources/knowledgebases/hg19/KnownFusionPairs.hg19.bedpe -breakend_pon /opt/resources/gridss_pon/hg19/gridss_pon_single_breakend.hg19.bed -breakpoint_pon /opt/resources/gridss_pon/hg19/gridss_pon_breakpoint.hg19.bedpe -input_vcf /data/output/tumor.gridss.unfiltered.vcf.gz -output_vcf /data/output/tumor.gridss.somatic.vcf.gz",
                "java -Xmx24G -cp /opt/tools/gripss/1.7/gripss.jar com.hartwig.hmftools.gripss.GripssHardFilterApplicationKt -input_vcf /data/output/tumor.gridss.somatic.vcf.gz -output_vcf /data/output/tumor.gridss.somatic.filtered.vcf.gz");
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
}