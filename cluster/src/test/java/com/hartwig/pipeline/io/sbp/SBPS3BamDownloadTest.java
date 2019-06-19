package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SBPS3BamDownloadTest {

    private static final int BAM_SIZE = 100;
    private static final int BAI_SIZE = 10;
    private static final ResultsDirectory NAMESPACED_RESULTS = ResultsDirectory.defaultDirectory();

    @Test
    public void setsContentSizeOnBamForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest firstRequest = request.getAllValues().get(0);
        assertThat(firstRequest.getMetadata().getContentLength()).isEqualTo(BAM_SIZE);
    }

    @Test
    public void setsBucketNameOnBamForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest firstRequest = request.getAllValues().get(0);
        assertThat(firstRequest.getBucketName()).isEqualTo("hmf-bam-storage");
    }

    @Test
    public void setsBucketKeyOnBamForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest firstRequest = request.getAllValues().get(0);
        assertThat(firstRequest.getKey()).isEqualTo("FR1234/test.bam");
    }

    @Test
    public void setsReadLimitToMaxInt() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest firstRequest = request.getAllValues().get(0);
        assertThat(firstRequest.getRequestClientOptions().getReadLimit()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void setsContentSizeOnBaiForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest secondRequest = request.getAllValues().get(1);
        assertThat(secondRequest.getMetadata().getContentLength()).isEqualTo(BAI_SIZE);
    }

    @Test
    public void setsBucketNameOnBaiForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest secondRequest = request.getAllValues().get(1);
        assertThat(secondRequest.getBucketName()).isEqualTo("hmf-bam-storage");
    }

    @Test
    public void setsBucketKeyOnBaiForTransfer() {
        ArgumentCaptor<PutObjectRequest> request = setupTransfer();
        PutObjectRequest secondRequest = request.getAllValues().get(1);
        assertThat(secondRequest.getKey()).isEqualTo("FR1234/test.bam.bai");
    }

    @Test
    public void retriesWholeTransferOnExceptions() throws Exception {
        TransferManager transferManager = mock(TransferManager.class);
        Upload upload = mock(Upload.class);
        when(transferManager.upload(any())).thenReturn(upload);
        doThrow(new AmazonClientException("test")).doNothing().when(upload).waitForCompletion();
        SBPS3BamDownload victim = victim(transferManager, 1);
        MockRuntimeBucket mockRuntimeBucket =
                MockRuntimeBucket.of("test").with("results/test.sorted.bam", BAM_SIZE).with("results/test.sorted.bam.bai", BAI_SIZE);
        victim.run(Sample.builder("", "test", "FR1234").build(), mockRuntimeBucket.getRuntimeBucket(), PipelineStatus.SUCCESS);
    }

    @NotNull
    public static SBPS3BamDownload victim(final TransferManager transferManager, final int retryDelay) {
        return new SBPS3BamDownload(transferManager, ResultsDirectory.defaultDirectory(), 3, retryDelay);
    }

    @NotNull
    private static ArgumentCaptor<PutObjectRequest> setupTransfer() {
        TransferManager transferManager = mock(TransferManager.class);
        ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
        Upload upload = mock(Upload.class);
        when(transferManager.upload(request.capture())).thenReturn(upload);
        SBPS3BamDownload victim = victim(transferManager, 0);
        MockRuntimeBucket mockRuntimeBucket = MockRuntimeBucket.of("test")
                .with(NAMESPACED_RESULTS.path("test.sorted.bam"), BAM_SIZE)
                .with(NAMESPACED_RESULTS.path("test.sorted.bam.bai"), BAI_SIZE);
        victim.run(Sample.builder("", "test", "FR1234").build(), mockRuntimeBucket.getRuntimeBucket(), PipelineStatus.SUCCESS);
        verify(transferManager, times(2)).upload(any());
        return request;
    }
}