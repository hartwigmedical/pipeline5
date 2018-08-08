package com.hartwig.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class DataLocationTest {

    private static final String SAMPLE_NAME = "TEST_SAMPLE";

    @Test
    public void pathFollowsConventionForSample() throws Exception {
        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.getUri()).thenReturn(new URI("file:/"));
        DataLocation victim = new DataLocation(fileSystem, "/results/");
        assertThat(victim.uri(OutputType.UNMAPPED, Sample.builder("", SAMPLE_NAME).name(SAMPLE_NAME).build())).isEqualTo(
                "file://results/TEST_SAMPLE_unmapped.bam");
    }
}