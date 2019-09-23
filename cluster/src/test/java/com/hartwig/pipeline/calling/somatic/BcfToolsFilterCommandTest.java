package com.hartwig.pipeline.calling.somatic;

import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static org.assertj.core.api.Assertions.assertThat;

public class BcfToolsFilterCommandTest {

    @Test
    public void createsBashToFilterOnPonWithBcfTools() {
        BcfToolsExcludeFilterCommand victim = new BcfToolsExcludeFilterCommand("'filter > 1'", "GERMLINE_PON", "input.vcf", "output.vcf");
        assertThat(victim.asBash()).isEqualTo(
                TOOLS_BCFTOOLS + " filter -e 'filter > 1' -s GERMLINE_PON -m+ input.vcf -O z -o output.vcf");
    }
}