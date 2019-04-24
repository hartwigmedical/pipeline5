package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class BcfToolsFilterCommandTest {

    @Test
    public void createsBashToFilterOnPonWithBcfTools() {
        BcfToolsExcludeFilterCommand victim = new BcfToolsExcludeFilterCommand("'filter > 1'", "GERMLINE_PON", "input.vcf", "output.vcf");
        assertThat(victim.asBash()).isEqualTo(
                "/data/tools/bcftools/1.3.1/bcftools filter -e 'filter > 1' -s GERMLINE_PON -m+ input.vcf -O z -o output.vcf");
    }
}