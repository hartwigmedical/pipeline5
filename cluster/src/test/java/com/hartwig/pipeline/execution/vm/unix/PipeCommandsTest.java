package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class PipeCommandsTest {
    @Test
    public void shouldStringCommandsTogetherWithPipes() {
        String cmdOne = "print_stuff";
        String cmdTwo = "process_it";
        String cmdThree = "write_it";

        PipeCommands pipeline = new PipeCommands(() -> {return cmdOne;}, () -> {return cmdTwo;}, () -> {return cmdThree;});
        assertThat(pipeline.asBash()).isEqualTo(format("%s | %s | %s", cmdOne, cmdTwo, cmdThree));
    }
}