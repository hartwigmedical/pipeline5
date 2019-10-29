package com.hartwig.pipeline.transfer.sbp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class CloudFileTest {
    private String md5;
    private Long size;
    private String path;
    private CloudFile file;

    @Before
    public void setup() {
        md5 = "1234567890abcdef1234567890abcdef";
        size = 123456789123456L;
        path = "path/to/file";
        file = CloudFile.builder().bucket("bucket").md5(md5).path(path).provider("gs").size(size).build();
    }

    @Test
    public void shouldReturnUrlRepresentation() {
        assertThat(file.toUrl()).isEqualTo("gs://bucket/" + path);
    }

    @Test
    public void shouldReturnManifestFormWithSizeFillingField() {
        assertThat(file.toManifestForm()).isEqualTo(md5 + " " + size + " " + path);
    }

    @Test
    public void shouldReturnManifestFormWithSizeRightJustified() {
        CloudFile withSmallFile = CloudFile.builder().from(file).size(1L).build();
        assertThat(withSmallFile.toManifestForm()).isEqualTo(md5 + "               1 " + path);
    }
}