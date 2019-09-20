package com.hartwig.pipeline.calling.substages;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class SnpEffTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpEff("snpeff.config");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.snpeff.annotated.vcf.gz");
    }

    @Test
    public void runsSnpEff() {
        assertThat(output.currentBash().asUnixString()).contains("/opt/tools/snpEff/4.3s/snpEff.sh /opt/tools/snpEff/4.3s/snpEff.jar "
                + "snpeff.config GRCh37.75 " + outFile("tumor.strelka.vcf") + " " + outFile("tumor.snpeff.annotated.vcf"));
    }

    @Test
    public void bgzipsOutput() {
        assertThat(output.currentBash()
                .asUnixString()).contains("/opt/tools/tabix/0.2.6/bgzip -f " + outFile("tumor.snpeff.annotated.vcf"));
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/opt/tools/tabix/0.2.6/tabix " + expectedPath() + " -p vcf");
    }
}