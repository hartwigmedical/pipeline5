package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SageHotspotsApplicationTest extends SubStageTest {

    @Override
    SubStage createVictim() {
        return new SageHotspotsApplication("known_hotspots.tsv",
                "coding_regions.bed",
                "reference_genome.fasta",
                "tumor.bam",
                "reference.bam",
                "tumor",
                "reference");
    }

    @Override
    String expectedPath() {
        return "/data/output/tumor.sage.hotspots.vcf.gz";
    }

    @Test
    public void runsSageHotspotApplication() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx8G -cp /data/tools/sage/1.1/sage.jar "
                + "com.hartwig.hmftools.sage.SageHotspotApplication -tumor tumor -tumor_bam tumor.bam -reference reference -reference_bam "
                + "reference.bam -known_hotspots known_hotspots.tsv -coding_regions coding_regions.bed -ref_genome reference_genome.fasta "
                + "-out /data/output/tumor.sage.hotspots.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash()
                .asUnixString()).contains("/data/tools/tabix/0.2.6/tabix /data/output/tumor.sage.hotspots.vcf.gz -p vcf");
    }
}