package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class JobCompleteTest {

    @Test
    public void createsBashToPutCompletionFileWithDateIntoOutputDirectory(){
        JobComplete victim = new JobComplete("flag");
        assertThat(victim.asBash()).isEqualTo("date > /data/output/flag");
    }
}