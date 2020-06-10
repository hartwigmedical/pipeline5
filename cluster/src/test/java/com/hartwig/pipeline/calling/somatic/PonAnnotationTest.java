package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class PonAnnotationTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new PonAnnotation("germline.pon", "GERMLINE_PON.vcf.gz", "GERMLINE_PON_COUNT");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.germline.pon.annotated.vcf.gz";
    }

    @Test
    public void runsBcfToolsPonAnnotation() {
        assertThat(bash()).contains("/opt/tools/bcftools/1.9/bcftools annotate -a "
                + "GERMLINE_PON.vcf.gz -c GERMLINE_PON_COUNT /data/output/tumor.strelka.vcf "
                + "-O z -o /data/output/tumor.germline.pon.annotated.vcf.gz");
    }

    @Test
    public void runsTabix() {
        assertThat(bash()).contains(
                "/opt/tools/tabix/0.2.6/tabix /data/output/tumor.germline.pon.annotated.vcf.gz -p vcf");
    }
}