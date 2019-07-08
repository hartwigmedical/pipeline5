package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.transfer.SbpS3FileSource;

import org.junit.Test;

public class SbpS3FileSourceTest {
    @Test
    public void usesHostNameFromArgumentsAsS3Host() {
        String file = "some/file.name";
        assertThat(new SbpS3FileSource().apply(file)).isEqualTo("s3://" + file);
    }

}
