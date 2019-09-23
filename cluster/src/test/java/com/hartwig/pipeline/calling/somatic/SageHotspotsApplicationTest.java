package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SAGE_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class SageHotspotsApplicationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageHotspotsApplication("known_hotspots.tsv",
                "coding_regions.bed",
                "reference_genome.fasta",
                "tumor.bam",
                "reference.bam",
                "tumor",
                "reference");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.sage.hotspots.vcf.gz");
    }

    @Test
    public void runsSageHotspotApplication() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx8G -cp " + TOOLS_SAGE_JAR
                + " com.hartwig.hmftools.sage.SageHotspotApplication -tumor tumor -tumor_bam tumor.bam -reference reference -reference_bam "
                + "reference.bam -known_hotspots known_hotspots.tsv -coding_regions coding_regions.bed -ref_genome reference_genome.fasta "
                + "-out " + outFile("tumor.sage.hotspots.vcf.gz"));
    }
}
