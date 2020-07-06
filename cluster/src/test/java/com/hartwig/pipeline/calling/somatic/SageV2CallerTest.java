package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SageV2CallerTest extends TertiaryStageTest<SomaticCallerOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SomaticCallerOutput, SomaticRunMetadata> createVictim() {
        return new SageV2Caller(TestInputs.defaultPair(), TestInputs.HG37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("unzip -d /opt/resources /opt/resources/snpeff/hg37/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx110G -cp /opt/tools/sage/2.2/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam -hotspots /opt/resources/sage/hg37/KnownHotspots.hg19.vcf.gz -panel_bed /opt/resources/sage/hg37/ActionableCodingPanel.hg19.bed.gz -high_confidence_bed /opt/resources/beds/hg37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/tumor.sage.somatic.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo)",
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.somatic.vcf.gz -O z -o /data/output/tumor.sage.pass.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pass.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/hg37/out_150_hg19.mappability.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.sage.pass.vcf.gz -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/hg37/SageGermlinePon.hg19.1000x.vcf.gz -c PON_COUNT,PON_MAX /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 10' -s PON -m+ /data/output/tumor.sage.pon.annotated.vcf.gz -O u | /opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 6' -s PON -m+ -O u | /opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 6' -s PON -m+ -O z -o /data/output/tumor.sage.pon.filter.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/snpEff.config GRCh37.75 /data/output/tumor.sage.pon.filter.vcf.gz /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf",
                "java -Xmx8G -cp /opt/tools/sage/2.2/sage.jar com.hartwig.hmftools.sage.SagePostProcessApplication -in /data/output/tumor.snpeff.annotated.vcf.gz -out /data/output/tumor.sage.somatic.filtered.vcf.gz -assembly hg19");
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
        return Arguments.testDefaultsBuilder().runSageCaller(false).build();
    }

}