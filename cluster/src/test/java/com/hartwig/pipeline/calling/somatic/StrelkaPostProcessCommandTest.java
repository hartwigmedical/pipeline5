package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class StrelkaPostProcessCommandTest {

    @Test
    public void createsBashToRunStrelkaPostProcess() {
        StrelkaPostProcessCommand victim = new StrelkaPostProcessCommand("input.vcf", "output.vcf", "high_conf.bed", "tumor", "tumor.bam");
        assertThat(victim.asBash()).isEqualTo("java -Xmx20G -jar /data/tools/strelka-post-process/1.4/strelka-post-process.jar"
                + " -v input.vcf -hc_bed high_conf.bed -t tumor -o output.vcf -b tumor.bam");
    }
}