package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RCloneCloudCopyTest {

    private static final String PATH_TO_RCLONE = "/path/to/rclone";
    private static final String LOCAL_SOURCE_FILE = "/source/file.gz";
    private static final String LOCAL_TARGET_FILE = "target/file.gz";
    private static final String GCP_REMOTE = "google";
    private static final String AWS_REMOTE = "aws";
    private ProcessBuilder processBuilder;
    private ArgumentCaptor<List<String>> commandCaptor;
    private RCloneCloudCopy victim;

    @Before
    public void setUp() throws Exception {
        processBuilder = mock(ProcessBuilder.class);
        Process process = mock(Process.class);
        when(processBuilder.start()).thenReturn(process);
        when(processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)).thenReturn(processBuilder);
        when(processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)).thenReturn(processBuilder);
        when(process.waitFor(anyLong(), any())).thenReturn(true);
        when(process.exitValue()).thenReturn(0);
        //noinspection unchecked
        commandCaptor = ArgumentCaptor.forClass(List.class);
        victim = new RCloneCloudCopy(PATH_TO_RCLONE, GCP_REMOTE, AWS_REMOTE, () -> processBuilder);
    }

    @Test
    public void usesRcloneFromSpecifiedPath() throws Exception {
        victim.copy(LOCAL_SOURCE_FILE, LOCAL_TARGET_FILE);
        verify(processBuilder, times(1)).command(commandCaptor.capture());
        assertThat(commandCaptor.getValue().get(0)).isEqualTo(PATH_TO_RCLONE + RCloneCloudCopy.RCLONE);
    }

    @Test
    public void shouldPassAppropriateSubCommandAndArguments() {
        victim.copy(LOCAL_SOURCE_FILE, LOCAL_TARGET_FILE);
        verify(processBuilder, times(1)).command(commandCaptor.capture());
        assertThat(commandCaptor.getValue().get(1)).isEqualTo("copyto");
        assertThat(commandCaptor.getValue().get(2)).isEqualTo("-c");
    }

    @Test
    public void passesLocalFileSystemThroughToRclone() {
        victim.copy(LOCAL_SOURCE_FILE, LOCAL_TARGET_FILE);
        verify(processBuilder, times(1)).command(commandCaptor.capture());
        assertSourceIs(LOCAL_SOURCE_FILE);
        assertDestIs(LOCAL_TARGET_FILE);
    }

    @Test
    public void substitutesGoogleRemoteForAnyGSPath() {
        victim.copy("gs://" + LOCAL_SOURCE_FILE, "gs://" + LOCAL_TARGET_FILE);
        verify(processBuilder, times(1)).command(commandCaptor.capture());
        assertSourceIs(GCP_REMOTE + ":" + LOCAL_SOURCE_FILE);
        assertDestIs(GCP_REMOTE + ":" + LOCAL_TARGET_FILE);
    }

    @Test
    public void substitutesS3RemoteForAnyS3Path() {
        victim.copy("s3://" + LOCAL_SOURCE_FILE, "s3://" + LOCAL_TARGET_FILE);
        verify(processBuilder, times(1)).command(commandCaptor.capture());
        assertSourceIs(AWS_REMOTE + ":" + LOCAL_SOURCE_FILE);
        assertDestIs(AWS_REMOTE + ":" + LOCAL_TARGET_FILE);
    }

    @Test(expected = RuntimeException.class)
    public void rethrowsAnyExceptionsRunningProcessAsRuntime() throws Exception {
        when(processBuilder.start()).thenThrow(new IOException());
        victim.copy(LOCAL_SOURCE_FILE, LOCAL_TARGET_FILE);
    }

    private void assertSourceIs(String expected) {
        assertThat(commandCaptor.getValue().get(3)).isEqualTo(expected);
    }

    private void assertDestIs(String expected) {
        assertThat(commandCaptor.getValue().get(4)).isEqualTo(expected);
    }
}