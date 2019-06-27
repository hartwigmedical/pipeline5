package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;

public class InputDownloadTest {

    private InputDownload victim;

    @Before
    public void setUp() throws Exception {
        victim = new InputDownload(GoogleStorageLocation.of("test", "path/to/input.file"));
    }

    @Test
    public void createsBashToCopyInputWithGsUtil() {
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp -n gs://test/path/to/input.file /data/input/input.file");
    }

    @Test
    public void createsLocalPathUsingSourceLocationAndConvention() {
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp -n gs://test/path/to/input.file /data/input/input.file");
    }

    @Test
    public void supportsCopyingOfInputDirectories() {
        victim = new InputDownload(GoogleStorageLocation.of("test", "path/to/input/dir", true));
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp -n gs://test/path/to/input/dir/* /data/input/");
    }

}