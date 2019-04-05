package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.io.sources.GoogleStorageSampleSource;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GoogleStorageSampleSourceTest {

    private static final String SAMPLE = "CPCT12345678";
    private static final ImmutableArguments ARGUMENTS = Arguments.testDefaultsBuilder().sampleId(SAMPLE).build();
    private SampleSource victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        victim = new GoogleStorageSampleSource(storage);
    }

    @Test(expected = IllegalArgumentException.class)
    public void patientIdArgumentMustBeSpecified() {
        victim.sample(Arguments.testDefaults());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentIfNoSamplesInBucket() {
        Bucket bucket = MockRuntimeBucket.of(SAMPLE).getRuntimeBucket().bucket();
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        victim.sample(ARGUMENTS);
    }

    @Test
    public void calculatesSizeOfAllFilesInSampleBucket() {
        Bucket bucket = MockRuntimeBucket.of(SAMPLE).with("fastq1_r1.gz", 1).with("fastq2_r2.gz", 10).getRuntimeBucket().bucket();
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        SampleData sample = victim.sample(ARGUMENTS);
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytesGZipped()).isEqualTo(11);
    }

    @Test
    public void fileSizeScaledDownWhenNotGzipped() {
        Bucket bucket = MockRuntimeBucket.of(SAMPLE).with("fastq1_r1", 4).with("fastq2_r2.gz", 40).getRuntimeBucket().bucket();
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        SampleData sample = victim.sample(ARGUMENTS);
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytesGZipped()).isEqualTo(41);
    }
}