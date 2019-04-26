package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SnpEffCommandTest {

    @Test
    public void createsBashToCallSnpEffWrapperScript() {
        SnpEffCommand victim = new SnpEffCommand("config", "input.vcf", "output.vcf");
        assertThat(victim.asBash()).isEqualTo(
                "/data/tools/snpEff/4.3s/snpEff.sh /data/tools/snpEff/4.3s/snpEff.jar config GRCh37.75 input.vcf output.vcf");
    }
}