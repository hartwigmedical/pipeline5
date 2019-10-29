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
        String jdkArgs =
                "-Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.5.2/gridss.jar";
        return ImmutableList.of("export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "mkdir -p /data/output/reference.bam.gridss.working",
                "java -Xmx8G " + jdkArgs
                        + " gridss.analysis.CollectGridssMetrics ASSUME_SORTED=true I=/data/input/reference.bam O=/data/output/reference.bam.gridss.working/reference.bam THRESHOLD_COVERAGE=50000 FILE_EXTENSION=null GRIDSS_PROGRAM=null PROGRAM=null PROGRAM=CollectInsertSizeMetrics STOP_AFTER=1000000",
                "(java -Xmx8G " + jdkArgs
                        + " gridss.CollectGridssMetricsAndExtractSVReads ASSUME_SORTED=true I=/data/input/reference.bam O=/data/output/reference.bam.gridss.working/reference.bam THRESHOLD_COVERAGE=50000 FILE_EXTENSION=null GRIDSS_PROGRAM=null GRIDSS_PROGRAM=CollectCigarMetrics GRIDSS_PROGRAM=CollectMapqMetrics GRIDSS_PROGRAM=CollectTagMetrics GRIDSS_PROGRAM=CollectIdsvMetrics GRIDSS_PROGRAM=ReportThresholdCoverage PROGRAM=null PROGRAM=CollectInsertSizeMetrics SV_OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 METRICS_OUTPUT=/data/output/reference.bam.gridss.working/reference.sv_metrics INSERT_SIZE_METRICS=/data/output/reference.bam.gridss.working/reference.bam.insert_size_metrics UNMAPPED_READS=false MIN_CLIP_LENGTH=5 INCLUDE_DUPLICATES=true | /opt/tools/sambamba/0.6.8/sambamba sort -m 8G -t $(grep -c '^processor' /proc/cpuinfo) -l 0 -o /data/output/gridss.tmp.querysorted.reference.sv.bam -n /dev/stdin)",
                "(java -Xmx8G " + jdkArgs
                        + " gridss.ComputeSamTags WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta COMPRESSION_LEVEL=0 I=/data/output/gridss.tmp.querysorted.reference.sv.bam O=/dev/stdout RECALCULATE_SA_SUPPLEMENTARY=true SOFTEN_HARD_CLIPS=true FIX_MATE_INFORMATION=true FIX_DUPLICATE_FLAG=true TAGS=null TAGS=NM TAGS=SA TAGS=R2 TAGS=Q2 TAGS=MC TAGS=MQ ASSUME_SORTED=true | /opt/tools/sambamba/0.6.8/sambamba sort -m 8G -t $(grep -c '^processor' /proc/cpuinfo) -l 0 -o /data/output/gridss.tmp.withtags.reference.sv.bam /dev/stdin)",
                "java -Xmx8G " + jdkArgs
                        + " gridss.SoftClipsToSplitReads WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta I=/data/output/gridss.tmp.withtags.reference.sv.bam O=/data/output/reference.bam.gridss.working/reference.bam.sv.bam",
                "mkdir -p /data/output/tumor.bam.gridss.working",
                "java -Xmx8G " + jdkArgs
                        + " gridss.analysis.CollectGridssMetrics ASSUME_SORTED=true I=/data/input/tumor.bam O=/data/output/tumor.bam.gridss.working/tumor.bam THRESHOLD_COVERAGE=50000 FILE_EXTENSION=null GRIDSS_PROGRAM=null PROGRAM=null PROGRAM=CollectInsertSizeMetrics STOP_AFTER=1000000",
                "(java -Xmx8G " + jdkArgs
                        + " gridss.CollectGridssMetricsAndExtractSVReads ASSUME_SORTED=true I=/data/input/tumor.bam O=/data/output/tumor.bam.gridss.working/tumor.bam THRESHOLD_COVERAGE=50000 FILE_EXTENSION=null GRIDSS_PROGRAM=null GRIDSS_PROGRAM=CollectCigarMetrics GRIDSS_PROGRAM=CollectMapqMetrics GRIDSS_PROGRAM=CollectTagMetrics GRIDSS_PROGRAM=CollectIdsvMetrics GRIDSS_PROGRAM=ReportThresholdCoverage PROGRAM=null PROGRAM=CollectInsertSizeMetrics SV_OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 METRICS_OUTPUT=/data/output/tumor.bam.gridss.working/tumor.sv_metrics INSERT_SIZE_METRICS=/data/output/tumor.bam.gridss.working/tumor.bam.insert_size_metrics UNMAPPED_READS=false MIN_CLIP_LENGTH=5 INCLUDE_DUPLICATES=true | /opt/tools/sambamba/0.6.8/sambamba sort -m 8G -t $(grep -c '^processor' /proc/cpuinfo) -l 0 -o /data/output/gridss.tmp.querysorted.tumor.sv.bam -n /dev/stdin)",
                "(java -Xmx8G " + jdkArgs
                        + " gridss.ComputeSamTags WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta COMPRESSION_LEVEL=0 I=/data/output/gridss.tmp.querysorted.tumor.sv.bam O=/dev/stdout RECALCULATE_SA_SUPPLEMENTARY=true SOFTEN_HARD_CLIPS=true FIX_MATE_INFORMATION=true FIX_DUPLICATE_FLAG=true TAGS=null TAGS=NM TAGS=SA TAGS=R2 TAGS=Q2 TAGS=MC TAGS=MQ ASSUME_SORTED=true | /opt/tools/sambamba/0.6.8/sambamba sort -m 8G -t $(grep -c '^processor' /proc/cpuinfo) -l 0 -o /data/output/gridss.tmp.withtags.tumor.sv.bam /dev/stdin)",
                "java -Xmx8G " + jdkArgs
                        + " gridss.SoftClipsToSplitReads WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta I=/data/output/gridss.tmp.withtags.tumor.sv.bam O=/data/output/tumor.bam.gridss.working/tumor.bam.sv.bam",
                "mkdir -p /data/output/reference_tumor.assemble.bam.gridss.working",
                "java -Xmx80G " + jdkArgs
                        + " gridss.AssembleBreakends WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta INPUT=/data/input/reference.bam INPUT=/data/input/tumor.bam OUTPUT=/data/output/reference_tumor.assemble.bam BLACKLIST=/data/resources/blacklist.bed CONFIGURATION_FILE=/data/resources/gridss.properties",
                "java -Xmx8G " + jdkArgs
                        + " gridss.analysis.CollectGridssMetrics ASSUME_SORTED=true I=/data/output/reference_tumor.assemble.bam O=/data/output/reference_tumor.assemble.bam.gridss.working/reference_tumor.assemble.bam THRESHOLD_COVERAGE=50000 FILE_EXTENSION=null GRIDSS_PROGRAM=null GRIDSS_PROGRAM=CollectCigarMetrics GRIDSS_PROGRAM=CollectMapqMetrics GRIDSS_PROGRAM=CollectTagMetrics GRIDSS_PROGRAM=CollectIdsvMetrics GRIDSS_PROGRAM=ReportThresholdCoverage PROGRAM=null PROGRAM=CollectInsertSizeMetrics",
                "java -Xmx8G " + jdkArgs
                        + " gridss.SoftClipsToSplitReads WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta I=/data/output/reference_tumor.assemble.bam O=/data/output/reference_tumor.assemble.bam.gridss.working/reference_tumor.assemble.bam.sv.bam REALIGN_ENTIRE_READ=true",
                "java -Xmx8G " + jdkArgs
                        + " gridss.IdentifyVariants WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta INPUT=/data/input/reference.bam INPUT=/data/input/tumor.bam OUTPUT_VCF=/data/output/reference_tumor.calling.vcf ASSEMBLY=/data/output/reference_tumor.assemble.bam BLACKLIST=/data/resources/blacklist.bed CONFIGURATION_FILE=/data/resources/gridss.properties",
                "java -Xmx8G " + jdkArgs
                        + " gridss.AnnotateVariants WORKING_DIR=/data/output REFERENCE_SEQUENCE=/data/resources/reference.fasta INPUT=/data/input/reference.bam INPUT=/data/input/tumor.bam INPUT_VCF=/data/output/reference_tumor.calling.vcf OUTPUT_VCF=/data/output/reference_tumor.annotated_variants.vcf ASSEMBLY=/data/output/reference_tumor.assemble.bam BLACKLIST=/data/resources/blacklist.bed CONFIGURATION_FILE=/data/resources/gridss.properties",
                "java -Xmx8G " + jdkArgs
                        + " gridss.AnnotateUntemplatedSequence REFERENCE_SEQUENCE=/data/resources/reference.fasta INPUT=/data/output/reference_tumor.annotated_variants.vcf OUTPUT=/data/output/reference_tumor.annotation.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference_tumor.annotation.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference_tumor.annotation.vcf.gz -p vcf",
                "/bin/bash -e /opt/tools/gridss/2.5.2/failsafe_repeatmasker_invoker.sh /data/output/reference_tumor.annotation.vcf.gz /data/output/reference_tumor.repeatmasker_annotation.vcf.gz /data/resources/gridss.hg19.fa.out /opt/tools/gridss/2.5.2",
                "gunzip -kd /data/output/reference_tumor.repeatmasker_annotation.vcf.gz",
                "(grep -E '^#' /data/output/reference_tumor.repeatmasker_annotation.vcf > /data/output/reference_tumor.repeatmasker_annotation.withbealn.vcf || true)",
                "cp /data/output/reference_tumor.repeatmasker_annotation.withbealn.vcf /data/output/reference_tumor.repeatmasker_annotation.missingbealn.vcf",
                "( (grep BEALN /data/output/reference_tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/reference_tumor.repeatmasker_annotation.withbealn.vcf || true) )",
                "( (grep -v BEALN /data/output/reference_tumor.repeatmasker_annotation.vcf || true) | (grep -vE '^#' >> /data/output/reference_tumor.repeatmasker_annotation.missingbealn.vcf || true) )",
                "java -Xmx8G -Dsamjdk.create_index=true -Dsamjdk.use_async_io_read_samtools=true -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=true -Dsamjdk.buffer_size=4194304 -cp /opt/tools/gridss/2.5.2/gridss.jar gridss.AnnotateUntemplatedSequence REFERENCE_SEQUENCE=/data/resources/human_virus.fa INPUT=/data/output/reference_tumor.repeatmasker_annotation.missingbealn.vcf OUTPUT=/data/output/reference_tumor.repeatmasker_annotation.withannotation.vcf",
                "java -Xmx32G -jar /opt/tools/picard/2.18.27/picard.jar SortVcf I=/data/output/reference_tumor.repeatmasker_annotation.withbealn.vcf I=/data/output/reference_tumor.repeatmasker_annotation.withannotation.vcf O=/data/output/reference_tumor.viral_annotation.vcf.gz",
                "(gunzip -c /data/output/reference_tumor.viral_annotation.vcf.gz | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", \":0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000:\")} ; print $0  } ' > /data/output/reference_tumor.viral_annotation.vcf)",
                "Rscript /opt/tools/gridss/2.5.2/gridss_somatic_filter.R -p /data/resources -i /data/output/reference_tumor.viral_annotation.vcf -o /data/output/tumor.gridss.somatic.vcf -f /data/output/tumor.gridss.somatic.full.vcf -s /opt/tools/gridss/2.5.2",
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