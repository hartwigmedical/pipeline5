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

public class SageGermlineCallerTest extends TertiaryStageTest<SageOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<SageOutput, SomaticRunMetadata> createVictim() {
        return new SageGermlineCaller(TestInputs.defaultPair(), TestInputs.REG_GENOME_37_RESOURCE_FILES, new NoopPersistedDataset());
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                "unzip -d /opt/resources /opt/resources/snpeff/37/snpEff_v4_3_GRCh37.75.zip",
                "java -Xmx110G -cp /opt/tools/sage/2.5/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor reference -tumor_bam /data/input/reference.bam -reference tumor -reference_bam /data/input/tumor.bam -hotspots /opt/resources/sage/37/KnownHotspots.germline.hg19.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.germline.hg19.bed.gz -hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 -hotspot_max_germline_rel_raw_base_qual 100 -panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 -mnv_filter_enabled false -high_confidence_bed /opt/resources/GIAB_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/tumor.sage.germline.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo) -panel_only",
                "(/opt/tools/bcftools/1.9/bcftools filter -i 'FILTER=\"PASS\"' /data/output/tumor.sage.germline.vcf.gz -O z -o /data/output/tumor.sage.pass.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.pass.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools view -s reference,tumor /data/output/tumor.sage.pass.vcf.gz -O z -o /data/output/tumor.sage.sort.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.sort.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/mappability/37/out_150_hg19.mappability.bed.gz -h /opt/resources/mappability/mappability.hdr -c CHROM,FROM,TO,-,MAPPABILITY /data/output/tumor.sage.sort.vcf.gz -O z -o /data/output/tumor.mappability.annotated.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.mappability.annotated.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/clinvar.hg19.vcf.gz -c INFO/CLNSIG,INFO/CLNSIGCONF /data/output/tumor.mappability.annotated.vcf.gz -O z -o /data/output/tumor.clinvar.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.clinvar.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.hg19.bed.gz -m BLACKLIST_BED -c CHROM,FROM,TO /data/output/tumor.clinvar.vcf.gz -O z -o /data/output/tumor.blacklist.regions.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.blacklist.regions.vcf.gz -p vcf",
                "(/opt/tools/bcftools/1.9/bcftools annotate -a /opt/resources/sage/37/KnownBlacklist.germline.hg19.vcf.gz -m BLACKLIST_VCF /data/output/tumor.blacklist.regions.vcf.gz -O z -o /data/output/tumor.blacklist.variants.vcf.gz)",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.blacklist.variants.vcf.gz -p vcf",
                "/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar /opt/resources/snpeff/37/snpEff.config GRCh37.75 /data/output/tumor.blacklist.variants.vcf.gz /data/output/tumor.sage.germline.filtered.vcf",
                "/opt/tools/tabix/0.2.6/bgzip -f /data/output/tumor.sage.germline.filtered.vcf",
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.sage.germline.filtered.vcf.gz -p vcf");
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
        return Arguments.testDefaultsBuilder().runSageGermlineCaller(false).build();
    }

    @Override
    protected void validatePersistedOutput(final SageOutput output) {
        assertThat(output.finalVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/sage_germline/tumor.sage.germline.filtered.vcf.gz"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // ignore for now
    }
}