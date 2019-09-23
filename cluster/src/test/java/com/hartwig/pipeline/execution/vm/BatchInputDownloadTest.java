package com.hartwig.pipeline.execution.vm;

import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.IN_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchInputDownloadTest {
    @Test
    public void shouldCreateSingleGsCopyCommandForAllInputsToInputDirectory() {
        InputDownload inputOne = mock(InputDownload.class);
        InputDownload inputTwo = mock(InputDownload.class);
        BatchInputDownload download = new BatchInputDownload(inputOne, inputTwo);
        when(inputOne.getRemoteSourcePath()).thenReturn("gs://remote/location.1");
        when(inputTwo.getRemoteSourcePath()).thenReturn("gs://remoter/location.2");

        String allInputSources = inputOne.getRemoteSourcePath() + " " + inputTwo.getRemoteSourcePath();
        assertThat(download.asBash()).isEqualTo("gsutil -qm cp " + allInputSources + " " + IN_DIR);
    }
}
