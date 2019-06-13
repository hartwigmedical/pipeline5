package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.io.sources.GoogleStorageSampleSource;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
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
        Bucket bucket = mock(Bucket.class);
        Page<Blob> pages = mock(Page.class);
        when(pages.iterateAll()).thenReturn(Lists.newArrayList());
        when(bucket.list(any())).thenReturn(pages);
        when(storage.get(anyString())).thenReturn(bucket);
        victim.sample(ARGUMENTS);
    }

    @Test
    public void calculatesSizeOfAllFilesInSampleBucket() {
        Bucket bucket = mock(Bucket.class);
        Blob firstBlob = mock(Blob.class);
        when(firstBlob.getSize()).thenReturn(1L);
        when(firstBlob.getName()).thenReturn("fastq1_r1.gz");
        Blob secondBlob = mock(Blob.class);
        when(secondBlob.getSize()).thenReturn(10L);
        when(secondBlob.getName()).thenReturn("fastq1_r2.gz");

        Page<Blob> blobs = mock(Page.class);
        when(blobs.iterateAll()).thenReturn(Lists.newArrayList(firstBlob, secondBlob));

        when(bucket.list(Storage.BlobListOption.prefix("aligner/samples/"))).thenReturn(blobs);
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        SampleData sample = victim.sample(ARGUMENTS);
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytesGZipped()).isEqualTo(11);
    }

    @Test
    public void fileSizeScaledDownWhenNotGzipped() {
        Bucket bucket = mock(Bucket.class);
        Blob firstBlob = mock(Blob.class);
        when(firstBlob.getSize()).thenReturn(4L);
        when(firstBlob.getName()).thenReturn("fastq1_r1");
        Blob secondBlob = mock(Blob.class);
        when(secondBlob.getSize()).thenReturn(40L);
        when(secondBlob.getName()).thenReturn("fastq1_r2.gz");

        Page<Blob> blobs = mock(Page.class);
        when(blobs.iterateAll()).thenReturn(Lists.newArrayList(firstBlob, secondBlob));

        when(bucket.list(Storage.BlobListOption.prefix("aligner/samples/"))).thenReturn(blobs);
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        SampleData sample = victim.sample(ARGUMENTS);
        assertThat(sample.sample().name()).isEqualTo(SAMPLE);
        assertThat(sample.sizeInBytesGZipped()).isEqualTo(41);
    }
}