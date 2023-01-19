package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.DeleteFilesCommand;

import org.junit.Test;

public class DeleteFilesCommandTest {
    @Test
    public void createsValidCommand() {
        List<String> filesToDelete = Arrays.asList("file1", "file2");
        assertThat(new DeleteFilesCommand(filesToDelete).asBash()).isEqualTo("rm file1 file2");
    }
}