package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CloudFileTest {
    @Test
    public void shouldReturnUrlRepresentation() {
        CloudFile file = CloudFile.builder().bucket("bucket").md5("md5").path("path/to/file").provider("gs").size(1L).build();
        assertThat(file.toUrl()).isEqualTo("gs://bucket/path/to/file");
    }
}