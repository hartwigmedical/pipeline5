package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SageHotspotAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SageHotspotAnnotation("known_hotspots.tsv", "sage_hotspots.vcf");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.sage.hotspots.annotated.vcf.gz";
    }

    @Test
    public void runsSageHotspotsAnnotation() {
        assertThat(bash()).contains("java -Xmx8G -cp /opt/tools/sage/2.1/sage.jar "
                + "com.hartwig.hmftools.sage.SageHotspotAnnotation -source_vcf /data/output/tumor.strelka.vcf -hotspot_vcf "
                + "sage_hotspots.vcf -known_hotspots known_hotspots.tsv -out /data/output/tumor.sage.hotspots.annotated.vcf.gz");
    }
}