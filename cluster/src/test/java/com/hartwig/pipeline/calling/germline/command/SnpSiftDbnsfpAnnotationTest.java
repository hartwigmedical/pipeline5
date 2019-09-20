package com.hartwig.pipeline.calling.germline.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;

import org.junit.Test;

public class SnpSiftDbnsfpAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpSiftDbnsfpAnnotation("dbnsfp.vcf.gz", "snpEff.config");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.dbnsfp.annotated.vcf.gz");
    }

    @Test
    public void runsSnpSiftDbsnfpAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("(java -Xmx20G -jar /opt/tools/snpEff/4.3s/SnpSift.jar dbnsfp -c "
                + "snpEff.config -v -f");
        assertThat(output.currentBash().asUnixString()).contains(
                "-db dbnsfp.vcf.gz " + outFile("tumor.strelka.vcf") + " > " + outFile("tumor.dbnsfp.annotated.vcf"));
    }

    @Test
    public void runsBgZip() {
        assertThat(output.currentBash().asUnixString()).contains("bgzip -f " + outFile("tumor.dbnsfp.annotated.vcf"));
    }
}