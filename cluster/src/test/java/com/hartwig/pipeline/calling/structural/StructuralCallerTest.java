package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestInputs.REFERENCE_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.TUMOR_BUCKET;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class StructuralCallerTest extends StageTest<StructuralCallerOutput, SomaticRunMetadata> {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.GRIDSS_CONFIG, "gridss.properties", "blacklist.bed");
        MockResource.addToStorage(storage, ResourceNames.GRIDSS_PON, "gridss.bed");
        MockResource.addToStorage(storage, ResourceNames.GRIDSS_REPEAT_MASKER_DB, "gridss.hg19.fa.out");
        MockResource.addToStorage(storage, ResourceNames.VIRUS_REFERENCE_GENOME, "human_virus.fa");
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runStructuralCaller(false).build();
    }

    @Override
    protected Stage<StructuralCallerOutput, SomaticRunMetadata> createVictim() {
        return new StructuralCaller(TestInputs.defaultPair());
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
    protected List<String> expectedResources() {
        return ImmutableList.of(resource("reference_genome"),
                resource("gridss_config"),
                resource("gridss_pon"),
                resource("gridss_repeatmasker_db"),
                resource("virus_reference_genome"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return TestInputs.SOMATIC_BUCKET;
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "export PATH=\"${PATH}:/opt/tools/samtools/1.9\"",
                "/opt/tools/gridss/2.7.2/gridss.sh -o /data/output/tumor.gridss.somatic.full.vcf -a /data/output/tumor.assembly.bam -w /data/output -r /data/resources/reference.fasta -j /opt/tools/gridss/2.7.2/gridss.jar -b /data/resources/blacklist.bed -c /data/resources/gridss.properties /data/input/reference.bam /data/input/tumor.bam",
                "/bin/bash -e /opt/tools/gridss/2.7.2/failsafe_repeatmasker_invoker.sh /data/output/tumor.gridss.somatic.full.vcf /data/output/tumor.repeatmasker_annotation.vcf.gz /data/resources/gridss.hg19.fa.out /opt/tools/gridss/2.7.2",
                "gunzip -kd /data/output/tumor.repeatmasker_annotation.vcf.gz",
                "(grep -E '^#' /data/output/tumor.repeatmasker_annotation.vcf > /data/output/tumor.repeatmasker_annotation.withbealn.vcf || true)",
                "cp /data/output/tumor.repeatmasker_annotation.withbealn.vcf /data/output/tumor.repeatmasker_annotation.missingbealn.vcf",
                "( (grep BEALN /data/output/tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/tumor.repeatmasker_annotation.withbealn.vcf || true) )",
                "( (grep -v BEALN /data/output/tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/tumor.repeatmasker_annotation.missingbealn.vcf || true) )",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.7.2/gridss.jar gridss.AnnotateUntemplatedSequence REFERENCE_SEQUENCE=/data/resources/human_virus.fa INPUT=/data/output/tumor.repeatmasker_annotation.missingbealn.vcf OUTPUT=/data/output/tumor.repeatmasker_annotation.withannotation.vcf",
                "java -Xmx32G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.7.2/gridss.jar picard.vcf.SortVcf I=/data/output/tumor.repeatmasker_annotation.withbealn.vcf I=/data/output/tumor.repeatmasker_annotation.withannotation.vcf O=/data/output/tumor.viral_annotation.vcf.gz",
                "(gunzip -c /data/output/tumor.viral_annotation.vcf.gz | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", \":0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000:\")} ; print $0  } ' > /data/output/tumor.viral_annotation.vcf)",
                "Rscript /opt/tools/gridss/2.7.2/gridss_somatic_filter.R -p /data/resources -i /data/output/tumor.viral_annotation.vcf -o /data/output/tumor.gridss.somatic.vcf -f /data/output/tumor.gridss.somatic.full.vcf -s /opt/tools/gridss/2.7.2",
                "mv /data/output/tumor.gridss.somatic.full.vcf.bgz /data/output/tumor.gridss.somatic.full.vcf.gz",
                "mv /data/output/tumor.gridss.somatic.full.vcf.bgz.tbi /data/output/tumor.gridss.somatic.full.vcf.gz.tbi",
                "mv /data/output/tumor.gridss.somatic.vcf.bgz /data/output/tumor.gridss.somatic.vcf.gz",
                "mv /data/output/tumor.gridss.somatic.vcf.bgz.tbi /data/output/tumor.gridss.somatic.vcf.gz.tbi");
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