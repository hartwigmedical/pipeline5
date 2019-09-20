package com.hartwig.pipeline.alignment.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class MergeMarkDupsTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new MergeMarkDups(Lists.newArrayList("tumor.l001.bam", "tumor.l002.bam"));
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.sorted.bam");
    }

    @Test
    public void markdupsWithSambamba() {
        assertThat(output.currentBash().asUnixString()).contains("/opt/tools/sambamba/0.6.8/sambamba markdup -t $(grep -c '^processor' "
                + "/proc/cpuinfo) --overflow-list-size=45000000 tumor.l001.bam tumor.l002.bam " + expectedPath());
    }
}