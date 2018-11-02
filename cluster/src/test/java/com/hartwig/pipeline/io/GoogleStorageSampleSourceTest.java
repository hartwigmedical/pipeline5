package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.bootstrap.ImmutableArguments;
import com.hartwig.pipeline.io.sources.GoogleStorageSampleSource;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class GoogleStorageSampleSourceTest {

    private static final String SAMPLE = "CPCT12345678";
    private static final ImmutableArguments ARGUMENTS = Arguments.defaultsBuilder().patientId(SAMPLE).build();
    private SampleSource victim;

    @Before
    public void setUp() throws Exception {
        victim = new GoogleStorageSampleSource();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentIfNoSamplesInBucket() {
        victim.sample(ARGUMENTS, MockRuntimeBucket.of(SAMPLE).getRuntimeBucket());
    }

    @Test
    public void calculatesSizeOfAllFilesInSampleBucket() {
        SampleData sample =
                victim.sample(ARGUMENTS, MockRuntimeBucket.of(SAMPLE).with("fastq1_r1.gz", 1).with("fastq2_r2.gz", 10).getRuntimeBucket());
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytes()).isEqualTo(11);
    }

    @Test
    public void fileSizeScaledDownWhenNotGzipped() {
        SampleData sample =
                victim.sample(ARGUMENTS, MockRuntimeBucket.of(SAMPLE).with("fastq1_r1", 4).with("fastq2_r2", 40).getRuntimeBucket());
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytes()).isEqualTo(11);
    }
}