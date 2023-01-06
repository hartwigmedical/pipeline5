package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class GermlineCallerTest extends StageTest<GermlineCallerOutput, SingleSampleRunMetadata> {

    public static final String REFERENCE_GERMLINE_VCF_GZ = "reference.germline.vcf.gz";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected Stage<GermlineCallerOutput, SingleSampleRunMetadata> createVictim() {
        return new GermlineCaller(TestInputs.referenceAlignmentOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/aligner/results/reference.bam", "reference.bam"),
                input(expectedRuntimeBucketName() + "/aligner/results/reference.bam.bai", "reference.bam.bai"));
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runGermlineCaller(false).build();
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
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
    protected List<String> expectedCommands() {
        return ImmutableList.<String>builder().add(
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.raw_germline_caller.vcf.gz --reference_sequence /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.raw_germline_caller.vcf.gz -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/reference.genotype_vcfs.vcf",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType SNP -selectType NO_VARIATION -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_snp.vcf",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_snp.vcf -o /data/output/reference.filtered_snp.vcf --filterExpression \"QD < 2.0\" --filterName \"SNP_LowQualityDepth\" --filterExpression \"MQ < 40.0\" --filterName \"SNP_MappingQuality\" --filterExpression \"FS > 60.0\" --filterName \"SNP_StrandBias\" --filterExpression \"HaplotypeScore > 13.0\" --filterName \"SNP_HaplotypeScoreHigh\" --filterExpression \"MQRankSum < -12.5\" --filterName \"SNP_MQRankSumLow\" --filterExpression \"ReadPosRankSum < -8.0\" --filterName \"SNP_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType INDEL -selectType MIXED -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_indels.vcf",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_indels.vcf -o /data/output/reference.filtered_indels.vcf --filterExpression \"QD < 2.0\" --filterName \"INDEL_LowQualityDepth\" --filterExpression \"FS > 200.0\" --filterName \"INDEL_StrandBias\" --filterExpression \"ReadPosRankSum < -20.0\" --filterName \"INDEL_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -V /data/output/reference.filtered_snp.vcf -V /data/output/reference.filtered_indels.vcf -o /data/output/reference.filtered_variants.vcf -R /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --assumeIdenticalSamples",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference.filtered_variants.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.filtered_variants.vcf.gz -p vcf",
                "mv /data/output/reference.filtered_variants.vcf.gz /data/output/" + REFERENCE_GERMLINE_VCF_GZ,
                "mv /data/output/reference.filtered_variants.vcf.gz.tbi /data/output/" + REFERENCE_GERMLINE_VCF_GZ + ".tbi").build();
    }

    @Override
    protected void validateOutput(final GermlineCallerOutput output) {
        // not supported currently
    }

    @Override
    protected void validatePersistedOutput(final GermlineCallerOutput output) {
        assertThat(output.germlineVcfLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/germline_caller/" + REFERENCE_GERMLINE_VCF_GZ));
        assertThat(output.germlineVcfIndexLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/germline_caller/" + REFERENCE_GERMLINE_VCF_GZ + ".tbi"));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.GERMLINE_VARIANTS, "germline_caller/" + REFERENCE_GERMLINE_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final GermlineCallerOutput output) {
        assertThat(output.germlineVcfLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "germline_caller/" + REFERENCE_GERMLINE_VCF_GZ));
        assertThat(output.germlineVcfIndexLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "germline_caller/" + REFERENCE_GERMLINE_VCF_GZ + ".tbi"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }
}