package com.hartwig.pipeline.io.sbp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.JobResult;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SBPS3BamDownloadTest {

    private static final int BAM_SIZE = 100;
    private static final int BAI_SIZE = 10;

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

    @NotNull
    private static ArgumentCaptor<PutObjectRequest> setupTransfer() {
        TransferManager transferManager = mock(TransferManager.class);
        ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
        Upload upload = mock(Upload.class);
        when(transferManager.upload(request.capture())).thenReturn(upload);
        SBPS3BamDownload victim = new SBPS3BamDownload(transferManager, ResultsDirectory.defaultDirectory());
        MockRuntimeBucket mockRuntimeBucket =
                MockRuntimeBucket.of("test").with("results/test.sorted.bam", BAM_SIZE).with("results/test.sorted.bam.bai", BAI_SIZE);
        victim.run(Sample.builder("", "test", "FR1234").build(), mockRuntimeBucket.getRuntimeBucket(), JobResult.SUCCESS);
        return request;
    }
}