package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GoogleStorageLocationTest {

    @Test
    public void convertGcsUrlToLocation () {
        GoogleStorageLocation victim = GoogleStorageLocation.from("gs://bucket/path/to/file");
        assertThat(victim.bucket()).isEqualTo("bucket");
        assertThat(victim.path()).isEqualTo("path/to/file");
    }
}