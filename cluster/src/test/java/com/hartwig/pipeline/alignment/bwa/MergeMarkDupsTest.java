package com.hartwig.pipeline.alignment.bwa;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;

import org.junit.Test;

public class MergeMarkDupsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new MergeMarkDups(Lists.newArrayList("tumor.l001.bam", "tumor.l002.bam"));
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.bam";
    }

    @Test
    public void markdupsWithSambamba() {
        assertThat(bash()).contains("/opt/tools/sambamba/0.6.8/sambamba markdup -t $(grep -c '^processor' "
                + "/proc/cpuinfo) --overflow-list-size=45000000 tumor.l001.bam tumor.l002.bam /data/output/tumor.bam");
    }
}