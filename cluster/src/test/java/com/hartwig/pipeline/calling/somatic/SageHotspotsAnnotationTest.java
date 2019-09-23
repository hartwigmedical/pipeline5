package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SAGE_JAR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class SageHotspotsAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageHotspotsAnnotation("known_hotspots.tsv", "sage_hotspots.vcf");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.sage.hotspots.annotated.vcf.gz");
    }

    @Test
    public void runsSageHotspotsAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx8G -cp " + TOOLS_SAGE_JAR
                + " com.hartwig.hmftools.sage.SageHotspotAnnotation -source_vcf " + outFile("tumor.strelka.vcf")
                + " -hotspot_vcf sage_hotspots.vcf -known_hotspots known_hotspots.tsv -out " + expectedPath());
    }
}