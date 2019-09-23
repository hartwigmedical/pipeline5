package com.hartwig.pipeline.execution.vm;

import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class JobCompleteTest {

    @Test
    public void createsBashToPutCompletionFileWithDateIntoOutputDirectory(){
        JobComplete victim = new JobComplete("flag");
        assertThat(victim.asBash()).isEqualTo("date > " + outFile("flag"));
    }
}