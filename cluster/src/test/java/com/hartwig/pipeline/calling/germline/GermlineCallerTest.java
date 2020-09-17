package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class GermlineCallerTest extends StageTest<GermlineCallerOutput, SingleSampleRunMetadata> {

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
        return new GermlineCaller(TestInputs.referenceAlignmentOutput(), TestInputs.HG19_RESOURCE_FILES, (m, r) -> Optional.empty());
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
        // not supported yet.
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.<String>builder().add("unzip -d /opt/resources /opt/resources/snpeff/hg19/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.raw_germline_caller.vcf.gz --reference_sequence /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.raw_germline_caller.vcf.gz -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType SNP -selectType NO_VARIATION -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_snp.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_snp.vcf -o /data/output/reference.filtered_snp.vcf --filterExpression \"QD < 2.0\" --filterName \"SNP_LowQualityDepth\" --filterExpression \"MQ < 40.0\" --filterName \"SNP_MappingQuality\" --filterExpression \"FS > 60.0\" --filterName \"SNP_StrandBias\" --filterExpression \"HaplotypeScore > 13.0\" --filterName \"SNP_HaplotypeScoreHigh\" --filterExpression \"MQRankSum < -12.5\" --filterName \"SNP_MQRankSumLow\" --filterExpression \"ReadPosRankSum < -8.0\" --filterName \"SNP_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType INDEL -selectType MIXED -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_indels.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_indels.vcf -o /data/output/reference.filtered_indels.vcf --filterExpression \"QD < 2.0\" --filterName \"INDEL_LowQualityDepth\" --filterExpression \"FS > 200.0\" --filterName \"INDEL_StrandBias\" --filterExpression \"ReadPosRankSum < -20.0\" --filterName \"INDEL_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -V /data/output/reference.filtered_snp.vcf -V /data/output/reference.filtered_indels.vcf -o /data/output/reference.filtered_variants.vcf -R /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta --assumeIdenticalSamples",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/hg19/snpEff.config GRCh37.75 /data/output/reference.filtered_variants.vcf /data/output/reference.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.snpeff.annotated.vcf.gz -p vcf",
                "mv /data/output/reference.snpeff.annotated.vcf.gz /data/output/reference.germline.vcf.gz",
                "mv /data/output/reference.snpeff.annotated.vcf.gz.tbi /data/output/reference.germline.vcf.gz.tbi").build();
    }

    @Override
    protected void validateOutput(final GermlineCallerOutput output) {
        // no additional
    }

    @Override
    protected void validatePersistedOutput(final GermlineCallerOutput output) {
        assertThat(output.germlineVcfLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/germline_caller/reference.germline.vcf.gz"));
        assertThat(output.germlineVcfIndexLocation()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/germline_caller/reference.germline.vcf.gz.tbi"));
    }
}