package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.JobCompleteCommand;

import org.junit.Test;

public class JobCompleteCommandTest {

    @Test
    public void createsBashToPutCompletionFileWithDateIntoOutputDirectory(){
        JobCompleteCommand victim = new JobCompleteCommand("flag");
        assertThat(victim.asBash()).isEqualTo("date > /data/output/flag");
    }
}