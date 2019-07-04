package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SBPS3FileSourceTest {
    @Test
    public void usesHostNameFromArgumentsAsS3Host() {
        String file = "some/file.name";
        assertThat(new SBPS3FileSource().apply(file)).isEqualTo("s3://" + file);
    }

}