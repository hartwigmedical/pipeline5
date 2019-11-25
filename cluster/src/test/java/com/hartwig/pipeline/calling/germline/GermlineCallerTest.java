package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

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
        return new GermlineCaller(TestInputs.referenceAlignmentOutput());
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

    @Test
    public void alsoDisabledWhenShallow() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().shallow(true).build())).isFalse();
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported yet.
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.<String>builder().add("unzip -d /opt/resources /opt/resources/snpeff/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.raw_germline_caller.vcf.gz -D /opt/resources/dbsnps/dbsnp_137.b37.vcf --reference_sequence /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.raw_germline_caller.vcf.gz -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -D /opt/resources/dbsnps/dbsnp_137.b37.vcf -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType SNP -selectType NO_VARIATION -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_snp.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_snp.vcf -o /data/output/reference.filtered_snp.vcf --filterExpression \"QD < 2.0\" --filterName \"SNP_LowQualityDepth\" --filterExpression \"MQ < 40.0\" --filterName \"SNP_MappingQuality\" --filterExpression \"FS > 60.0\" --filterName \"SNP_StrandBias\" --filterExpression \"HaplotypeScore > 13.0\" --filterName \"SNP_HaplotypeScoreHigh\" --filterExpression \"MQRankSum < -12.5\" --filterName \"SNP_MQRankSumLow\" --filterExpression \"ReadPosRankSum < -8.0\" --filterName \"SNP_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType INDEL -selectType MIXED -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_indels.vcf",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -V /data/output/reference.raw_indels.vcf -o /data/output/reference.filtered_indels.vcf --filterExpression \"QD < 2.0\" --filterName \"INDEL_LowQualityDepth\" --filterExpression \"FS > 200.0\" --filterName \"INDEL_StrandBias\" --filterExpression \"ReadPosRankSum < -20.0\" --filterName \"INDEL_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx29G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -V /data/output/reference.filtered_snp.vcf -V /data/output/reference.filtered_indels.vcf -o /data/output/reference.filtered_variants.vcf -R /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta --assumeIdenticalSamples",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/snpEff.config GRCh37.75 /data/output/reference.filtered_variants.vcf /data/output/reference.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.snpeff.annotated.vcf.gz -p vcf",
                "(java -Xmx29G -jar /opt/tools/snpEff/4.3s/SnpSift.jar dbnsfp -c /opt/resources/snpeff/snpEff.config -v -f hg38_chr,hg38_pos,genename,Uniprot_acc,Uniprot_id,Uniprot_aapos,Interpro_domain,cds_strand,refcodon,SLR_test_statistic,codonpos,fold-degenerate,Ancestral_allele,Ensembl_geneid,Ensembl_transcriptid,aapos,aapos_SIFT,aapos_FATHMM,SIFT_score,SIFT_converted_rankscore,SIFT_pred,Polyphen2_HDIV_score,Polyphen2_HDIV_rankscore,Polyphen2_HDIV_pred,Polyphen2_HVAR_score,Polyphen2_HVAR_rankscore,Polyphen2_HVAR_pred,LRT_score,LRT_converted_rankscore,LRT_pred,MutationTaster_score,MutationTaster_converted_rankscore,MutationTaster_pred,MutationAssessor_score,MutationAssessor_rankscore,MutationAssessor_pred,FATHMM_score,FATHMM_rankscore,FATHMM_pred,MetaSVM_score,MetaSVM_rankscore,MetaSVM_pred,MetaLR_score,MetaLR_rankscore,MetaLR_pred,Reliability_index,VEST3_score,VEST3_rankscore,PROVEAN_score,PROVEAN_converted_rankscore,PROVEAN_pred,CADD_raw,CADD_raw_rankscore,CADD_phred,GERP++_NR,GERP++_RS,GERP++_RS_rankscore,phyloP46way_primate,phyloP46way_primate_rankscore,phyloP46way_placental,phyloP46way_placental_rankscore,phyloP100way_vertebrate,phyloP100way_vertebrate_rankscore,phastCons46way_primate,phastCons46way_primate_rankscore,phastCons46way_placental,phastCons46way_placental_rankscore,phastCons100way_vertebrate,phastCons100way_vertebrate_rankscore,SiPhy_29way_pi,SiPhy_29way_logOdds,SiPhy_29way_logOdds_rankscore,LRT_Omega,UniSNP_ids,1000Gp1_AC,1000Gp1_AF,1000Gp1_AFR_AC,1000Gp1_AFR_AF,1000Gp1_EUR_AC,1000Gp1_EUR_AF,1000Gp1_AMR_AC,1000Gp1_AMR_AF,1000Gp1_ASN_AC,1000Gp1_ASN_AF,ESP6500_AA_AF,ESP6500_EA_AF,ARIC5606_AA_AC,ARIC5606_AA_AF,ARIC5606_EA_AC,ARIC5606_EA_AF,ExAC_AC,ExAC_AF,ExAC_Adj_AC,ExAC_Adj_AF,ExAC_AFR_AC,ExAC_AFR_AF,ExAC_AMR_AC,ExAC_AMR_AF,ExAC_EAS_AC,ExAC_EAS_AF,ExAC_FIN_AC,ExAC_FIN_AF,ExAC_NFE_AC,ExAC_NFE_AF,ExAC_SAS_AC,ExAC_SAS_AF,clinvar_rs,clinvar_clnsig,clinvar_trait,COSMIC_ID,COSMIC_CNT -db /opt/resources/dbsnps/dbsnp_137.b37.vcf /data/output/reference.snpeff.annotated.vcf.gz > /data/output/reference.dbnsfp.annotated.vcf)",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference.dbnsfp.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.dbnsfp.annotated.vcf.gz -p vcf",
                "/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/cosmic_v85/CosmicCodingMuts_v85_collapsed.vcf.gz -c ID -o /data/output/reference.cosmic.annotated.vcf.gz -O z /data/output/reference.dbnsfp.annotated.vcf.gz",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.cosmic.annotated.vcf.gz -p vcf",
                "(java -Xmx29G -jar /opt/tools/snpEff/4.3s/SnpSift.jar annotate -c /opt/resources/snpeff/snpEff.config -tabix -name GoNLv5_ -info AF,AN,AC /opt/resources/gonl_v5/gonl.snps_indels.r5.sorted.vcf.gz /data/output/reference.cosmic.annotated.vcf.gz > /data/output/reference.gonlv5.annotated.vcf)",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/reference.gonlv5.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/reference.gonlv5.annotated.vcf.gz -p vcf",
                "mv /data/output/reference.gonlv5.annotated.vcf.gz /data/output/reference.germline.vcf.gz",
                "mv /data/output/reference.gonlv5.annotated.vcf.gz.tbi /data/output/reference.germline.vcf.gz.tbi").build();
    }

    @Override
    protected void validateOutput(final GermlineCallerOutput output) {
        // no additional
    }
}