package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.execution.vm.command.BatchInputDownloadCommand;
import com.hartwig.pipeline.execution.vm.command.InputDownloadCommand;

import org.junit.Test;

public class BatchInputDownloadCommandCommandTest {
    @Test
    public void shouldCreateSingleGsCopyCommandForAllInputsToInputDirectory() {
        InputDownloadCommand inputOne = mock(InputDownloadCommand.class);
        InputDownloadCommand inputTwo = mock(InputDownloadCommand.class);
        BatchInputDownloadCommand download = new BatchInputDownloadCommand(inputOne, inputTwo);
        when(inputOne.getRemoteSourcePath()).thenReturn("gs://remote/location.1");
        when(inputTwo.getRemoteSourcePath()).thenReturn("gs://remoter/location.2");

        String allInputSources = inputOne.getRemoteSourcePath() + " " + inputTwo.getRemoteSourcePath();
        assertThat(download.asBash()).isEqualTo("gsutil -qm cp " + allInputSources + " " + "/data/input");
    }
}
