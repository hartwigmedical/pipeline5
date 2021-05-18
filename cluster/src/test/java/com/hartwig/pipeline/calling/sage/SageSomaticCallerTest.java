package com.hartwig.pipeline.calling.sage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SageSomaticCallerTest extends TertiaryStageTest<SageOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SageOutput, SomaticRunMetadata> createVictim() {
        return new SageSomaticCaller(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, new NoopPersistedDataset(), false);
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of("unzip -d /opt/resources /opt/resources/snpeff/37/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx110G -cp /opt/tools/sage/2.8/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam -hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/tumor.sage.somatic.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo) -coverage_bed /opt/resources/sage/37/ActionableCodingPanel.somatic.37.bed.gz",
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.somatic.vcf.gz -O z -o /data/output/tumor.sage.pass.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pass.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/37/out_150.mappability.37.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.sage.pass.vcf.gz -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/SageGermlinePon.1000x.37.vcf.gz -c PON_COUNT,PON_MAX /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.sage.pon.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 10' -s PON -m+ /data/output/tumor.sage.pon.annotated.vcf.gz -O u | /opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 6' -s PON -m+ -O u | /opt/tools/bcftools/1.9/bcftools filter -e 'PON_COUNT!=\".\" && INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 6' -s PON -m+ -O z -o /data/output/tumor.sage.pon.filter.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pon.filter.vcf.gz -p vcf",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/snpEff.config GRCh37.75 /data/output/tumor.sage.pon.filter.vcf.gz /data/output/tumor.sage.somatic.filtered.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.sage.somatic.filtered.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.somatic.filtered.vcf.gz -p vcf");
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported currently
    }

    @Override
    protected void validateOutput(final SageOutput output) {
        // not supported currently
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSomaticCaller(false).build();
    }

    @Override
    protected void validatePersistedOutput(final SageOutput output) {
        assertThat(output.finalVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/sage_somatic/tumor.sage.somatic.filtered.vcf.gz"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // ignore for now
    }
}