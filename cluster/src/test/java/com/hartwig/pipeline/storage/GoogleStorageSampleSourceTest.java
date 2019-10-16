package com.hartwig.pipeline.storage;

import static com.hartwig.pipeline.testsupport.TestInputs.referenceRunMetadata;

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
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ImmutableArguments;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class GoogleStorageSampleSourceTest {

    private static final ImmutableArguments ARGUMENTS = Arguments.testDefaultsBuilder().sampleId(TestInputs.referenceSample()).build();
    private SampleSource victim;
    private Storage storage;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        victim = new GoogleStorageSampleSource(storage, ARGUMENTS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentIfNoSamplesInBucket() {
        Bucket bucket = mock(Bucket.class);
        Page<Blob> pages = mock(Page.class);
        when(pages.iterateAll()).thenReturn(Lists.newArrayList());
        when(bucket.list(any())).thenReturn(pages);
        when(storage.get(anyString())).thenReturn(bucket);
        victim.sample(referenceRunMetadata());
    }

    @Test
    public void findsSampleInExistingRuntimeBucket() {
        verifyFastq(TestInputs.referenceSample() + "_HJJLGCCXX_S11_L001_R1_001.fastq.gz",
                TestInputs.referenceSample() + "_HJJLGCCXX_S11_L001_R2_001.fastq.gz");
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentExceptionWhenFileNamingIncorrect() {
        verifyFastq(TestInputs.referenceSample() + "_not_correct.fastq.gz",
                TestInputs.referenceSample() + "_HJJLGCCXX_S11_L001_R2_001.fastq.gz");
    }

    private void verifyFastq(final String firstOfPair, final String secondOfPair) {
        Bucket bucket = mock(Bucket.class);
        Blob firstBlob = TestBlobs.blob(firstOfPair);
        Blob secondBlob = TestBlobs.blob(secondOfPair);

        Page<Blob> page = TestBlobs.pageOf(firstBlob, secondBlob);
        when(bucket.list(Storage.BlobListOption.prefix("aligner/samples/"))).thenReturn(page);
        when(storage.get(Mockito.anyString())).thenReturn(bucket);
        Sample sample = victim.sample(referenceRunMetadata());
        assertThat(sample.name()).isEqualTo(TestInputs.referenceSample());
        assertThat(sample.lanes()).hasSize(1);
        Lane lane = sample.lanes().get(0);
        assertThat(lane.firstOfPairPath()).isEqualTo(firstBlob.getName());
        assertThat(lane.secondOfPairPath()).isEqualTo(secondBlob.getName());
    }

}