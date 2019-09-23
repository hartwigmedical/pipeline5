package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.storage.GoogleStorageLocation;
import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.IN_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.inFile;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(victim.asBash()).isEqualTo(format("gsutil -qm cp -n gs://%s/%s %s", bucket, remoteSourcePath, inFile("input.file")));
    }

    @Test
    public void supportsCopyingOfInputDirectories() {
        victim = new InputDownload(GoogleStorageLocation.of(bucket, "path/to/input/dir", true));
        assertThat(victim.asBash()).isEqualTo(format("gsutil -qm cp -n gs://%s/path/to/input/dir/* %s/", bucket, IN_DIR));
    }

    @Test
    public void shouldReturnRemoteSourcePath() {
        assertThat(victim.getRemoteSourcePath()).isEqualTo("gs://" + bucket + "/" + remoteSourcePath);
    }
}