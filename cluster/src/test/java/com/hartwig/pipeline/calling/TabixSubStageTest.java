package com.hartwig.pipeline.calling;

import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.LOG_FILE;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_TABIX;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TabixSubStageTest extends SubStageTest {
    @Test
    public void runsTabix() {
        assertThat(output.currentBash()
                .asUnixString()).contains(TOOLS_TABIX + " " + expectedPath() + " -p vcf >>" + LOG_FILE);
    }
}
