package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SomaticCallerTest extends TertiaryStageTest<SomaticCallerOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SomaticCallerOutput, SomaticRunMetadata> createVictim() {
        return new SomaticCaller(TestInputs.defaultPair(), TestInputs.HG37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "java -Xmx8G -cp /opt/tools/sage/2.1/sage.jar com.hartwig.hmftools.sage.SageHotspotApplication -tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam -known_hotspots /opt/resources/sage/KnownHotspots.tsv -coding_regions /opt/resources/sage/CodingRegions.bed -ref_genome /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/tumor.sage.hotspots.vcf.gz",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.hotspots.vcf.gz -O u | /opt/tools/bcftools/1.3.1/bcftools annotate -x INFO/HOTSPOT -O u | /opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/LOW_CONFIDENCE -O u | /opt/tools/bcftools/1.3.1/bcftools annotate -x FILTER/GERMLINE_INDEL -O u | /opt/tools/bcftools/1.3.1/bcftools view -s tumor -O z -o /data/output/tumor.sage.hotspots.filtered.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.filtered.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/sage/hg37/SAGE_PON.vcf.gz -c SAGE_PON_COUNT /data/output/tumor.sage.hotspots.filtered.vcf.gz -O z -o /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools filter -e 'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0' -s SAGE_PON -m+ /data/output/tumor.sage.hotspots.pon.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.filter.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf",
                "unzip -d /opt/resources /opt/resources/snpeff/hg37/snpEff_v4_3_GRCh37.75.zip",
                "/opt/tools/strelka/1.0.14/bin/configureStrelkaWorkflow.pl --tumor /data/input/tumor.bam --normal /data/input/reference.bam --config /opt/resources/strelka_config/strelka_config_bwa_genome.ini --ref /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output-dir /data/output/strelkaAnalysis",
                "make -C /data/output/strelkaAnalysis -j $(grep -c '^processor' /proc/cpuinfo)",
                "java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T CombineVariants -R /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta --genotypemergeoption unsorted -V:snvs /data/output/strelkaAnalysis/results/passed.somatic.snvs.vcf -V:indels /data/output/strelkaAnalysis/results/passed.somatic.indels.vcf -o /data/output/tumor.strelka.vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/mappability/hg37/out_150_hg19.mappability.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.strelka.vcf -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/pon/GERMLINE_PON.vcf.gz -c GERMLINE_PON_COUNT /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.germline.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.germline.pon.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/pon/SOMATIC_PON.vcf.gz -c SOMATIC_PON_COUNT /data/output/tumor.germline.pon.annotated.vcf.gz -O z -o /data/output/tumor.somatic.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.somatic.pon.annotated.vcf.gz -p vcf",
                "java -Xmx20G -jar /opt/tools/strelka-post-process/1.6/strelka-post-process.jar -v /data/output/tumor.somatic.pon.annotated.vcf.gz -hc_bed /opt/resources/beds/hg37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -t tumor -o /data/output/tumor.strelka.post.processed.vcf.gz -b /data/input/tumor.bam",
                "(/opt/tools/bcftools/1.3.1/bcftools filter -e 'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ /data/output/tumor.strelka.post.processed.vcf.gz -O u | /opt/tools/bcftools/1.3.1/bcftools filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON -m+ -O z -o /data/output/tumor.pon.filtered.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.pon.filtered.vcf.gz -p vcf",
                "java -Xmx8G -cp /opt/tools/sage/2.1/sage.jar com.hartwig.hmftools.sage.SageHotspotAnnotation -source_vcf /data/output/tumor.pon.filtered.vcf.gz -hotspot_vcf /data/output/tumor.sage.pon.filter.vcf.gz -known_hotspots /opt/resources/sage/KnownHotspots.tsv -out /data/output/tumor.sage.hotspots.annotated.vcf.gz",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/snpEff.config GRCh37.75 /data/output/tumor.sage.hotspots.annotated.vcf.gz /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/cosmic_v85/CosmicCodingMuts_v85_collapsed.vcf.gz -c ID,INFO /data/output/tumor.snpeff.annotated.vcf.gz -O z -o /data/output/tumor.cosmic.annotated.final.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.cosmic.annotated.final.vcf.gz -p vcf");
    }

    @Override
    public void returnsExpectedOutput() {
        // ignored for now.
    }

    @Override
    protected void validateOutput(final SomaticCallerOutput output) {
        // ignored for now.
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSomaticCaller(false).build();
    }


}