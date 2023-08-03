package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import com.hartwig.pipeline.execution.vm.command.DeleteFilesCommand;

import org.junit.Test;

public class DeleteFilesCommandTest {
    @Test
    public void createsValidCommand() {
        assertThat(new DeleteFilesCommand(Arrays.asList("file1", "file2")).asBash()).isEqualTo("rm file1 file2");
    }
}