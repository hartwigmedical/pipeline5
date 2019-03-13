package com.hartwig.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.patient.Sample;
import com.hartwig.support.hadoop.Hadoop;

import org.junit.Test;

public class FinalDataLocationTest {

    @Test
    public void uriHasNoStageSuffixWhenNonePassed() throws Exception {
        FinalDataLocation victim = new FinalDataLocation(Hadoop.localFilesystem(), "results");
        String uri = victim.uri(Sample.builder("directory", "name").build(), "");
        assertThat(uri).isEqualTo("file:///results/name.bam");
    }

    @Test
    public void uriHasAddsSuffixAfterSampleBeforeExtension() throws Exception {
        FinalDataLocation victim = new FinalDataLocation(Hadoop.localFilesystem(), "results");
        String uri = victim.uri(Sample.builder("directory", "name").build(), "suffix");
        assertThat(uri).isEqualTo("file:///results/name.suffix.bam");
    }
}