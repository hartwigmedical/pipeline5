package com.hartwig.pipeline.calling.germline.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SnpSiftDbnsfpAnnotationTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new SnpSiftDbnsfpAnnotation("dbnsfp.vcf.gz", "snpEff.config");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.dbnsfp.annotated.vcf.gz";
    }

    @Test
    public void runsSnpSiftDbsnfpAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("(java -Xmx20G -jar /data/tools/snpEff/4.3s/SnpSift.jar dbnsfp -c "
                + "snpEff.config -v -f ");
        assertThat(output.currentBash().asUnixString()).contains("-db dbnsfp.vcf.gz /data/output/tumor.strelka.vcf > "
                + "/data/output/tumor.dbnsfp.annotated.vcf)");
    }

    @Test
    public void runsBgZip() {
        assertThat(output.currentBash().asUnixString()).contains("bgzip -f /data/output/tumor.dbnsfp.annotated.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains("tabix /data/output/tumor.dbnsfp.annotated.vcf.gz -p vcf");
    }
}