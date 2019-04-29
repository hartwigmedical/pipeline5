package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SageHotspotsAnnotationTest extends SubStageTest {

    @Override
    SubStage createVictim() {
        return new SageHotspotsAnnotation("known_hotspots.tsv", "sage_hotspots.vcf");
    }

    @Override
    String expectedPath() {
        return "/data/output/tumor.sage.hotspots.annotated.vcf.gz";
    }

    @Test
    public void runsSageHotspotsAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx8G -cp /data/tools/sage/1.1/sage.jar "
                + "com.hartwig.hmftools.sage.SageHotspotAnnotation -source_vcf /data/output/tumor.strelka.vcf -hotspot_vcf "
                + "sage_hotspots.vcf -known_hotspots known_hotspots.tsv -out /data/output/tumor.sage.hotspots.annotated.vcf.gz");
    }
}