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
    protected Stage<StructuralCallerOutput, SomaticRunMetadata> createVictim()
    {
        return new StructuralCaller(TestInputs.defaultPair(), TestInputs.HG37_RESOURCE);
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
                "export PATH=\"${PATH}:/opt/tools/samtools/1.9\"",
                "/opt/tools/gridss/2.7.2/gridss.sh -o /data/output/tumor.gridss.unfiltered.vcf.gz -a /data/output/tumor.assembly.bam -w /data/output -r /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -j /opt/tools/gridss/2.7.2/gridss.jar -b /opt/resources/gridss_config/ENCFF001TDO.bed -c /opt/resources/gridss_config/gridss.properties /data/input/reference.bam /data/input/tumor.bam",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.gridss.unfiltered.vcf.gz -p vcf",
                "/bin/bash -e /opt/tools/gridss/2.7.2/failsafe_repeatmasker_invoker.sh /data/output/tumor.gridss.unfiltered.vcf.gz /data/output/tumor.repeatmasker_annotation.vcf.gz /opt/resources/gridss_repeatmasker_db/hg19.fa.out /opt/tools/gridss/2.7.2",
                "gunzip -kd /data/output/tumor.repeatmasker_annotation.vcf.gz",
                "(grep -E '^#' /data/output/tumor.repeatmasker_annotation.vcf > /data/output/tumor.repeatmasker_annotation.withbealn.vcf || true)",
                "cp /data/output/tumor.repeatmasker_annotation.withbealn.vcf /data/output/tumor.repeatmasker_annotation.missingbealn.vcf",
                "( (grep BEALN /data/output/tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/tumor.repeatmasker_annotation.withbealn.vcf || true) )",
                "( (grep -v BEALN /data/output/tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/tumor.repeatmasker_annotation.missingbealn.vcf || true) )",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.7.2/gridss.jar gridss.AnnotateUntemplatedSequence REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa INPUT=/data/output/tumor.repeatmasker_annotation.missingbealn.vcf OUTPUT=/data/output/tumor.repeatmasker_annotation.withannotation.vcf",
                "java -Xmx32G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.7.2/gridss.jar picard.vcf.SortVcf I=/data/output/tumor.repeatmasker_annotation.withbealn.vcf I=/data/output/tumor.repeatmasker_annotation.withannotation.vcf O=/data/output/tumor.viral_annotation.vcf.gz",
                "(gunzip -c /data/output/tumor.viral_annotation.vcf.gz | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", \":0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000:\")} ; print $0  } ' > /data/output/tumor.viral_annotation.vcf)",
                "Rscript /opt/tools/gridss/2.7.2/gridss_somatic_filter.R -p /opt/resources/gridss_pon -i /data/output/tumor.viral_annotation.vcf -o /data/output/tumor.gridss.somatic.filtered.vcf -f /data/output/tumor.gridss.somatic.vcf -s /opt/tools/gridss/2.7.2",
                "mv /data/output/tumor.gridss.somatic.vcf.bgz /data/output/tumor.gridss.somatic.vcf.gz",
                "mv /data/output/tumor.gridss.somatic.vcf.bgz.tbi /data/output/tumor.gridss.somatic.vcf.gz.tbi",
                "mv /data/output/tumor.gridss.somatic.filtered.vcf.bgz /data/output/tumor.gridss.somatic.filtered.vcf.gz",
                "mv /data/output/tumor.gridss.somatic.filtered.vcf.bgz.tbi /data/output/tumor.gridss.somatic.filtered.vcf.gz.tbi");
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