package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SageV2CallerTest extends TertiaryStageTest<SageV2CallerOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SageV2CallerOutput, SomaticRunMetadata> createVictim() {
        return new SageV2Caller(TestInputs.defaultPair());
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("unzip -d /opt/resources /opt/resources/snpeff/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx110G -cp /opt/tools/sage/2.0/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam -hotspots /opt/resources/sage/KnownHotspots.hg19.vcf.gz -panel_bed /opt/resources/sage/ActionableCodingPanel.hg19.bed.gz -high_confidence_bed /opt/resources/beds/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed -ref_genome /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/tumor.sage.somatic.vcf.gz -threads $(grep -c '^processor' /proc/cpuinfo)",
                "(/opt/tools/bcftools/1.3.1/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.somatic.vcf.gz -O u | /opt/tools/bcftools/1.3.1/bcftools view -s tumor -O z -o /data/output/tumor.sage.pass.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pass.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/mappability/out_150_hg19.mappability.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.sage.pass.vcf.gz -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/sage/SageGermlinePon.hg19.vcf.gz -c PON_COUNT /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.3.1/bcftools filter -e 'PON_COUNT!= \".\" && (MIN(PON_COUNT) > 9 || (MIN(PON_COUNT) > 2 && INFO/TIER!=\"HOTSPOT\"))' -s SAGE_PON -m+ /data/output/tumor.sage.pon.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.filter.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/snpEff.config GRCh37.75 /data/output/tumor.sage.pon.filter.vcf.gz /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf",
                "java -Xmx8G -cp /opt/tools/sage/2.0/sage.jar com.hartwig.hmftools.sage.SagePostProcessApplication -in /data/output/tumor.snpeff.annotated.vcf.gz -out /data/output/tumor.sage.post.processed.vcf.gz -assembly hg19",
                "(/opt/tools/bcftools/1.3.1/bcftools annotate -a /opt/resources/cosmic_v85/CosmicCodingMuts_v85_collapsed.vcf.gz -c ID,INFO /data/output/tumor.sage.post.processed.vcf.gz -O z -o /data/output/tumor.cosmic.annotated.final.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.cosmic.annotated.final.vcf.gz -p vcf");
    }

    @Override
    public void returnsExpectedOutput() {
        // ignored for now.
    }

    @Override
    protected void validateOutput(final SageV2CallerOutput output) {
        // ignored for now.
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSageCaller(false).build();
    }

}