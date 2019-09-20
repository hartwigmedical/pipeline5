package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.testsupport.CommonTestEntities;

import org.junit.Test;

public class JobCompleteTest implements CommonTestEntities {

    @Test
    public void createsBashToPutCompletionFileWithDateIntoOutputDirectory(){
        JobComplete victim = new JobComplete("flag");
        assertThat(victim.asBash()).isEqualTo("date > " + outFile("flag"));
    }
}