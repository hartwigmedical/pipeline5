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
        return "/data/output/tumor.snpeff.annotated.vcf.gz";
    }

    @Test
    public void runsSnpEff() {
        assertThat(output.currentBash().asUnixString()).contains("/data/tools/snpEff/4.3s/snpEff.sh /data/tools/snpEff/4.3s/snpEff.jar "
                + "snpeff.config GRCh37.75 /data/output/tumor.strelka.vcf /" + "data/output/tumor.snpeff.annotated.vcf");
    }

    @Test
    public void bgzipsOutput() {
        assertThat(output.currentBash()
                .asUnixString()).contains("/data/tools/tabix/0.2.6/bgzip -f /data/output/tumor.snpeff.annotated.vcf");
    }

    @Test
    public void runsTabix() {
        assertThat(output.currentBash().asUnixString()).contains(
                "/data/tools/tabix/0.2.6/tabix /data/output/tumor.snpeff.annotated.vcf.gz -p vcf");
    }
}