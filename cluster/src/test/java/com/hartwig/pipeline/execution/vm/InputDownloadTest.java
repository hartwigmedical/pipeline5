package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;

public class InputDownloadTest {

    private GoogleStorageLocation googleStorageLocation;
    private InputDownload victim;

    @Before
    public void setUp() throws Exception {
        googleStorageLocation = GoogleStorageLocation.of("test", "path/to/input.file");
        victim = new InputDownload(googleStorageLocation);
    }

    @Test
    public void createsBashToCopyInputWithGsUtil() {
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp gs://test/path/to/input.file /data/inputs/input.file");
    }

    @Test
    public void createsLocalPathUsingSourceLocationAndConvention() {
        assertThat(victim.asBash()).isEqualTo("gsutil -qm cp gs://test/path/to/input.file /data/inputs/input.file");
    }

}