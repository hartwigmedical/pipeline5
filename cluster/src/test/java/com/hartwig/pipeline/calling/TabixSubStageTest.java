package com.hartwig.pipeline.calling;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public abstract class TabixSubStageTest extends SubStageTest {
    @Test
    public void runsTabix() {
        assertThat(output.currentBash()
                .asUnixString()).contains("/opt/tools/tabix/0.2.6/tabix " + expectedPath() + " -p vcf >>" + LOG_FILE);
    }
}
