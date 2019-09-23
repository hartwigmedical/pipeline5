package com.hartwig.pipeline.alignment.vm;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.testsupport.TestConstants;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.PROC_COUNT;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SAMBAMBA;
import static org.assertj.core.api.Assertions.assertThat;

public class MergeMarkDupsTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new MergeMarkDups(Lists.newArrayList("tumor.l001.bam", "tumor.l002.bam"));
    }

    @Override
    public String expectedPath() {
        return TestConstants.outFile("tumor.sorted.bam");
    }

    @Test
    public void markdupsWithSambamba() {
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_SAMBAMBA + " markdup -t " + PROC_COUNT +
                " --overflow-list-size=45000000 tumor.l001.bam tumor.l002.bam " + expectedPath());
    }
}