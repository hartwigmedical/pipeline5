package com.hartwig.pipeline.calling.germline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class GermlineCallerTest extends StageTest<GermlineCallerOutput, SingleSampleRunMetadata> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.DBNSFP, "dbsnfp.txt.gz");
        MockResource.addToStorage(storage, ResourceNames.GONL, "gonl.vcf.gz");
        MockResource.addToStorage(storage, ResourceNames.COSMIC, "cosmic_collapsed.vcf.gz");
        MockResource.addToStorage(storage, ResourceNames.SNPEFF, "snpeff.config", "database.zip");
        MockResource.addToStorage(storage, ResourceNames.DBSNPS, "dbsnps.vcf");
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
    protected List<String> expectedResources() {
        return ImmutableList.of(resource("reference_genome"),
                resource("dbsnps"),
                resource("snpeff"),
                resource("dbNSFP_v2.9"),
                resource("cosmic_v85"),
                resource("gonl_v5"));
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
        return ImmutableList.<String>builder().add("unzip -d /data/resources /data/resources/database.zip",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.germline_calling.vcf.gz -D /data/resources/dbsnps.vcf --reference_sequence /data/resources/reference.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.germline_calling.vcf.gz -R /data/resources/reference.fasta -D /data/resources/dbsnps.vcf -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.germline_calling.vcf.gz -D /data/resources/dbsnps.vcf --reference_sequence /data/resources/reference.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.germline_calling.vcf.gz -R /data/resources/reference.fasta -D /data/resources/dbsnps.vcf -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType SNP -selectType NO_VARIATION -R /data/resources/reference.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_snp.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /data/resources/reference.fasta -V /data/output/reference.raw_snp.vcf -o /data/output/reference.filtered_snp.vcf --filterExpression \"QD < 2.0\" --filterName \"SNP_LowQualityDepth\" --filterExpression \"MQ < 40.0\" --filterName \"SNP_MappingQuality\" --filterExpression \"FS > 60.0\" --filterName \"SNP_StrandBias\" --filterExpression \"HaplotypeScore > 13.0\" --filterName \"SNP_HaplotypeScoreHigh\" --filterExpression \"MQRankSum < -12.5\" --filterName \"SNP_MQRankSumLow\" --filterExpression \"ReadPosRankSum < -8.0\" --filterName \"SNP_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.germline_calling.vcf.gz -D /data/resources/dbsnps.vcf --reference_sequence /data/resources/reference.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.germline_calling.vcf.gz -R /data/resources/reference.fasta -D /data/resources/dbsnps.vcf -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType INDEL -selectType MIXED -R /data/resources/reference.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_indels.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /data/resources/reference.fasta -V /data/output/reference.raw_indels.vcf -o /data/output/reference.filtered_indels.vcf --filterExpression \"QD < 2.0\" --filterName \"INDEL_LowQualityDepth\" --filterExpression \"FS > 200.0\" --filterName \"INDEL_StrandBias\" --filterExpression \"ReadPosRankSum < -20.0\" --filterName \"INDEL_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T HaplotypeCaller -nct $(grep -c '^processor' /proc/cpuinfo) --input_file /data/input/reference.bam -o /data/output/reference.germline_calling.vcf.gz -D /data/resources/dbsnps.vcf --reference_sequence /data/resources/reference.fasta -variant_index_type LINEAR -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T GenotypeGVCFs -V /data/output/reference.germline_calling.vcf.gz -R /data/resources/reference.fasta -D /data/resources/dbsnps.vcf -o /data/output/reference.genotype_vcfs.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T SelectVariants -selectType SNP -selectType NO_VARIATION -R /data/resources/reference.fasta -V /data/output/reference.genotype_vcfs.vcf -o /data/output/reference.raw_snp.vcf",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T VariantFiltration -R /data/resources/reference.fasta -V /data/output/reference.raw_snp.vcf -o /data/output/reference.filtered_snp.vcf --filterExpression \"QD < 2.0\" --filterName \"SNP_LowQualityDepth\" --filterExpression \"MQ < 40.0\" --filterName \"SNP_MappingQuality\" --filterExpression \"FS > 60.0\" --filterName \"SNP_StrandBias\" --filterExpression \"HaplotypeScore > 13.0\" --filterName \"SNP_HaplotypeScoreHigh\" --filterExpression \"MQRankSum < -12.5\" --filterName \"SNP_MQRankSumLow\" --filterExpression \"ReadPosRankSum < -8.0\" --filterName \"SNP_ReadPosRankSumLow\" --clusterSize 3 --clusterWindowSize 35",
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -V /data/output/reference.filtered_snp.vcf -V /data/output/reference.filtered_indels.vcf -o /data/output/reference.filtered_variants.vcf -R /data/resources/reference.fasta --assumeIdenticalSamples",
                "$TOOLS_DIR/snpEff/4.3s/snpEff.sh $TOOLS_DIR/snpEff/4.3s/snpEff.jar /data/resources/snpeff.config GRCh37.75 /data/output/reference.filtered_variants.vcf /data/output/reference.snpeff.annotated.vcf",
                "$TOOLS_DIR/tabix/0.2.6/bgzip -f /data/output/reference.snpeff.annotated.vcf",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/reference.snpeff.annotated.vcf.gz -p vcf",
                "(java -Xmx20G -jar $TOOLS_DIR/snpEff/4.3s/SnpSift.jar dbnsfp -c /data/resources/snpeff.config -v -f hg38_chr,hg38_pos,genename,Uniprot_acc,Uniprot_id,Uniprot_aapos,Interpro_domain,cds_strand,refcodon,SLR_test_statistic,codonpos,fold-degenerate,Ancestral_allele,Ensembl_geneid,Ensembl_transcriptid,aapos,aapos_SIFT,aapos_FATHMM,SIFT_score,SIFT_converted_rankscore,SIFT_pred,Polyphen2_HDIV_score,Polyphen2_HDIV_rankscore,Polyphen2_HDIV_pred,Polyphen2_HVAR_score,Polyphen2_HVAR_rankscore,Polyphen2_HVAR_pred,LRT_score,LRT_converted_rankscore,LRT_pred,MutationTaster_score,MutationTaster_converted_rankscore,MutationTaster_pred,MutationAssessor_score,MutationAssessor_rankscore,MutationAssessor_pred,FATHMM_score,FATHMM_rankscore,FATHMM_pred,MetaSVM_score,MetaSVM_rankscore,MetaSVM_pred,MetaLR_score,MetaLR_rankscore,MetaLR_pred,Reliability_index,VEST3_score,VEST3_rankscore,PROVEAN_score,PROVEAN_converted_rankscore,PROVEAN_pred,CADD_raw,CADD_raw_rankscore,CADD_phred,GERP++_NR,GERP++_RS,GERP++_RS_rankscore,phyloP46way_primate,phyloP46way_primate_rankscore,phyloP46way_placental,phyloP46way_placental_rankscore,phyloP100way_vertebrate,phyloP100way_vertebrate_rankscore,phastCons46way_primate,phastCons46way_primate_rankscore,phastCons46way_placental,phastCons46way_placental_rankscore,phastCons100way_vertebrate,phastCons100way_vertebrate_rankscore,SiPhy_29way_pi,SiPhy_29way_logOdds,SiPhy_29way_logOdds_rankscore,LRT_Omega,UniSNP_ids,1000Gp1_AC,1000Gp1_AF,1000Gp1_AFR_AC,1000Gp1_AFR_AF,1000Gp1_EUR_AC,1000Gp1_EUR_AF,1000Gp1_AMR_AC,1000Gp1_AMR_AF,1000Gp1_ASN_AC,1000Gp1_ASN_AF,ESP6500_AA_AF,ESP6500_EA_AF,ARIC5606_AA_AC,ARIC5606_AA_AF,ARIC5606_EA_AC,ARIC5606_EA_AF,ExAC_AC,ExAC_AF,ExAC_Adj_AC,ExAC_Adj_AF,ExAC_AFR_AC,ExAC_AFR_AF,ExAC_AMR_AC,ExAC_AMR_AF,ExAC_EAS_AC,ExAC_EAS_AF,ExAC_FIN_AC,ExAC_FIN_AF,ExAC_NFE_AC,ExAC_NFE_AF,ExAC_SAS_AC,ExAC_SAS_AF,clinvar_rs,clinvar_clnsig,clinvar_trait,COSMIC_ID,COSMIC_CNT -db /data/resources/dbsnfp.txt.gz /data/output/reference.snpeff.annotated.vcf.gz > /data/output/reference.dbnsfp.annotated.vcf)",
                "$TOOLS_DIR/tabix/0.2.6/bgzip -f /data/output/reference.dbnsfp.annotated.vcf",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/reference.dbnsfp.annotated.vcf.gz -p vcf",
                "$TOOLS_DIR/bcftools/1.3.1/bcftools annotate -a /data/resources/cosmic_collapsed.vcf.gz -c ID -o /data/output/reference.cosmic.annotated.vcf.gz -O z /data/output/reference.dbnsfp.annotated.vcf.gz",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/reference.cosmic.annotated.vcf.gz -p vcf",
                "(java -Xmx20G -jar $TOOLS_DIR/snpEff/4.3s/SnpSift.jar annotate -c /data/resources/snpeff.config -tabix -name GoNLv5_ -info AF,AN,AC /data/resources/gonl.vcf.gz /data/output/reference.cosmic.annotated.vcf.gz > /data/output/reference.gonlv5.annotated.final.vcf)",
                "$TOOLS_DIR/tabix/0.2.6/bgzip -f /data/output/reference.gonlv5.annotated.final.vcf",
                "$TOOLS_DIR/tabix/0.2.6/tabix /data/output/reference.gonlv5.annotated.final.vcf.gz -p vcf").build();
    }

    @Override
    protected void validateOutput(final GermlineCallerOutput output) {
        // no additional
    }
}