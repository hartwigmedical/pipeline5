package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TabixCommandTest {

    @Test
    public void createsBashForTabixVcfIndexing() {
        TabixCommand victim = new TabixCommand("/path/to/unindexed.vcf");
        assertThat(victim.asBash()).isEqualTo("/data/tools/tabix/0.2.6/tabix /path/to/unindexed.vcf -p vcf");
    }
}