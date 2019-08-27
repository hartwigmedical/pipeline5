package com.hartwig.pipeline.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FullSomaticResultsTest {

    private static final String OUTPUT_BUCKET = "output-bucket";
    private Storage storage;
    private FullSomaticResults victim;
    private Bucket outputBucket;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        Arguments arguments = Arguments.testDefaultsBuilder().patientReportBucket(OUTPUT_BUCKET).build();
        victim = new FullSomaticResults(storage, arguments, 1);
        outputBucket = mock(Bucket.class);
        when(storage.get(OUTPUT_BUCKET)).thenReturn(outputBucket);
    }

    @Test
    public void copiesSingleSampleReferenceAndTumorBucketIntoSomatic() {

        Blob reference = returnSampleFromBucket(outputBucket, "reference");
        Blob tumor = returnSampleFromBucket(outputBucket, "tumor");

        ArgumentCaptor<Storage.CopyRequest> copyRequestArgumentCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        final CopyWriter copyWriter = mock(CopyWriter.class);
        when(copyWriter.getResult()).thenReturn(reference).thenReturn(tumor);
        when(storage.copy(copyRequestArgumentCaptor.capture())).thenReturn(copyWriter);

        victim.compose(TestInputs.defaultSomaticRunMetadata());

        assertThat(copyRequestArgumentCaptor.getAllValues().get(0).getSource().getName()).isEqualTo("reference/reference/output.txt");
        assertThat(copyRequestArgumentCaptor.getAllValues().get(0).getTarget().getName()).isEqualTo("run/reference/output.txt");

        assertThat(copyRequestArgumentCaptor.getAllValues().get(1).getSource().getName()).isEqualTo("tumor/tumor/output.txt");
        assertThat(copyRequestArgumentCaptor.getAllValues().get(1).getTarget().getName()).isEqualTo("run/tumor/output.txt");
    }

    @Test
    public void waitsForSingleSampleStagingComplete() {
        Blob reference = returnSampleOnSecondAttempt(outputBucket, "reference");
        Blob tumor = returnSampleOnSecondAttempt(outputBucket, "tumor");
        ArgumentCaptor<Storage.CopyRequest> copyRequestArgumentCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        final CopyWriter copyWriter = mock(CopyWriter.class);
        when(copyWriter.getResult()).thenReturn(reference).thenReturn(tumor);
        when(storage.copy(copyRequestArgumentCaptor.capture())).thenReturn(copyWriter);
        victim.compose(TestInputs.defaultSomaticRunMetadata());
        verify(outputBucket, times(2)).get("reference/STAGED");
        verify(outputBucket, times(2)).get("tumor/STAGED");
    }

    private static Blob returnSampleOnSecondAttempt(final Bucket outputBucket, final String sample) {
        Blob completion = TestBlobs.blob(sample + "/" + PipelineResults.STAGING_COMPLETE);
        Blob content = TestBlobs.blob(sample + "/" + sample + "/output.txt");
        Page<Blob> page = TestBlobs.pageOf(content);
        when(outputBucket.get(completion.getName())).thenReturn(null).thenReturn(completion);
        when(outputBucket.list(Storage.BlobListOption.prefix(sample))).thenReturn(page);
        return content;
    }

    private static Blob returnSampleFromBucket(final Bucket outputBucket, final String sample) {
        Blob blob = TestBlobs.blob(sample + "/" + sample + "/output.txt");
        Page<Blob> page = TestBlobs.pageOf(blob);
        when(outputBucket.list(Storage.BlobListOption.prefix(sample))).thenReturn(page);
        return blob;
    }

}