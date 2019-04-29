package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class PonAnnotationTest extends SubStageTest{

    @Override
    SubStage createVictim() {
        return new PonAnnotation("germline.pon", "GERMLINE_PON.vcf.gz", "GERMLINE_PON_COUNT");
    }

    @Override
    String expectedPath() {
        return "/data/output/tumor.germline.pon.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsPonAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/bcftools/1.3.1/bcftools annotate -a "
                + "GERMLINE_PON.vcf.gz -c GERMLINE_PON_COUNT -o /data/output/tumor.germline.pon.annotated.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/data/tools/tabix/0.2.6/tabix /data/output/tumor.germline.pon.annotated.vcf.gz -p vcf");
    }
}