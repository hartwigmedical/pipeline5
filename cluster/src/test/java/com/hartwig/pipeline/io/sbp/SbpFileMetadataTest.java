package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class SbpFileMetadataTest {
    private final String filename = "file/name";
    private final String directory = "directory";
    private final long filesize = 32L;
    private final String hash = "1234567890abcdef";
    private final int runId = 5;
    private SbpFileMetadata metadata;

    @Before
    public void setup() {
        metadata = SbpFileMetadata.builder().filename(filename).directory(directory).filesize(filesize).hash(hash).run_id(runId).build();
    }

    @Test
    public void shouldSetRunId() {
        assertThat(metadata.run_id()).isEqualTo(runId);
    }

    @Test
    public void shouldSetHash() {
        assertThat(metadata.hash()).isEqualTo(hash);
    }

    @Test
    public void shouldSetFilesize() {
        assertThat(metadata.filesize()).isEqualTo(filesize);
    }

    @Test
    public void shouldSetDirectory() {
        assertThat(metadata.directory()).isEqualTo(directory);
    }

    @Test
    public void shouldSetFilename() {
        assertThat(metadata.filename()).isEqualTo(filename);
    }
}