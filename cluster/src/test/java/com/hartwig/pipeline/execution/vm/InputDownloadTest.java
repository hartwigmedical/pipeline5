package com.hartwig.pipeline.execution.vm;

import static com.hartwig.pipeline.testsupport.TestInputs.inputDownload;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;

public class InputDownloadTest {

    private InputDownload victim;
    private String remoteSourcePath;
    private String bucket;

    @Before
    public void setUp() throws Exception {
        remoteSourcePath = "path/to/input.file";
        bucket = "test";
        victim = new InputDownload(GoogleStorageLocation.of(bucket, remoteSourcePath));
    }

    @Test
    public void createsBashToCopyInputWithGsUtil() {
        assertThat(victim.asBash()).isEqualTo(inputDownload(
                "cp -r -n gs://" + bucket + "/" + remoteSourcePath + " /data/input/input.file"));
    }

    @Test
    public void createsLocalPathUsingSourceLocationAndConvention() {
        assertThat(victim.asBash()).isEqualTo(inputDownload(
                "cp -r -n gs://" + bucket + "/" + remoteSourcePath + " /data/input/input.file"));
    }

    @Test
    public void supportsCopyingOfInputDirectories() {
        victim = new InputDownload(GoogleStorageLocation.of(bucket, "path/to/input/dir", true));
        assertThat(victim.asBash()).isEqualTo(inputDownload("cp -r -n gs://" + bucket + "/path/to/input/dir/* /data/input/"));
    }

    @Test
    public void shouldReturnRemoteSourcePath() {
        assertThat(victim.getRemoteSourcePath()).isEqualTo("gs://" + bucket + "/" + remoteSourcePath);
    }
}