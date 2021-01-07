package com.hartwig.pipeline.metadata;

import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRunResult;
import com.hartwig.pipeline.sbpapi.SbpRunResultUpdate;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.transfer.google.GoogleArchiver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpSomaticMetadataApiTest {

    private static final SbpRunResult FAILED = SbpRunResult.from(PipelineStatus.FAILED);
    private static final SbpRunResult SUCCESS = SbpRunResult.from(PipelineStatus.SUCCESS);

    private static final int RUN_ID = 1;
    private static final String SAMPLE_ID = "7141";
    private static final Storage.BlobListOption PREFIX = Storage.BlobListOption.prefix(TestInputs.defaultSomaticRunMetadata().set() + "/");
    private SomaticMetadataApi victim;
    private SbpRestApi sbpRestApi;
    private SomaticRunMetadata somaticRunMetadata;
    private PipelineState pipelineState;
    private Bucket sourceBucket;
    private GoogleArchiver googleArchiver;
    private ArgumentCaptor<String> entityId;
    private ArgumentCaptor<SbpRunResultUpdate> status;

    @Before
    public void setUp() throws Exception {
        sbpRestApi = mock(SbpRestApi.class);
        sourceBucket = mock(Bucket.class);
        Page<Blob> empty = pageOf();
        when(sourceBucket.list(PREFIX)).thenReturn(empty);
        somaticRunMetadata = TestInputs.defaultSomaticRunMetadata();
        googleArchiver = mock(GoogleArchiver.class);
        when(sbpRestApi.getInis()).thenReturn(TestJson.get("get_inis"));
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run"));
        entityId = ArgumentCaptor.forClass(String.class);
        status = ArgumentCaptor.forClass(SbpRunResultUpdate.class);
        pipelineState = mock(PipelineState.class);
        when(pipelineState.status()).thenReturn(PipelineStatus.SUCCESS);
        victim = new SbpSomaticMetadataApi(Arguments.testDefaults(), RUN_ID, sbpRestApi, sourceBucket, googleArchiver);
    }

    @Test
    public void retrievesSetMetadataFromSbpRestApi() {
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.reference().entityId()).isEqualTo(49);
        assertThat(setMetadata.tumor().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.tumor().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.tumor().entityId()).isEqualTo(50);
        assertThat(setMetadata.tumor().primaryTumorDoids()).containsOnly("1234", "5678");
    }

    @Test
    public void mapsSuccessStatusToSnpCheck() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(SbpSomaticMetadataApi.SNP_CHECK);
        assertThat(status.getValue().result()).contains(SUCCESS);
    }

    @Test
    public void mapsSuccessStatusToSuccessWhenShallow() {
        victim = new SbpSomaticMetadataApi(Arguments.testDefaultsBuilder().shallow(true).build(), RUN_ID,
                sbpRestApi,
                sourceBucket,
                googleArchiver);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(2)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(SbpSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().result()).contains(SUCCESS);
    }

    @Test
    public void mapsFailedStatusToPipeline5Finished() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sbpRestApi, times(1)).updateRunResult(entityId.capture(), status.capture());
        assertThat(entityId.getValue()).isEqualTo(String.valueOf(RUN_ID));
        assertThat(status.getValue().status()).isEqualTo(SbpSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().result()).contains(FAILED);
    }

    @Test
    public void handlesSingleSampleSet() {
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run_single_sample"));
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        SomaticRunMetadata setMetadata = victim.get();
        assertThat(setMetadata.set()).isEqualTo("170724_HMFregCPCT_FR13999246_FR13999144_CPCT02290012");
        assertThat(setMetadata.reference().sampleName()).isEqualTo("ZR17SQ1-00649");
        assertThat(setMetadata.reference().barcode()).isEqualTo("FR13257296");
        assertThat(setMetadata.maybeTumor()).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionWhenTumorIsMissing() {
        when(sbpRestApi.getSample(SAMPLE_ID)).thenReturn(TestJson.get("get_samples_by_set_single_sample"));
        victim.get();
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalStateIfNoBucketInRun() {
        when(pipelineState.status()).thenReturn(PipelineStatus.FAILED);
        when(sbpRestApi.getRun(RUN_ID)).thenReturn(TestJson.get("get_run_no_bucket"));
        victim.complete(pipelineState, somaticRunMetadata);
    }

    @Test
    public void shouldIterateThroughOutputObjects() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(sourceBucket).list(PREFIX);
    }

    @Test
    public void setsRunToFailedAndRethrowsIfSbpTransferFails() {
        doThrow(new RuntimeException()).when(sourceBucket).list(PREFIX);
        try {
            victim.complete(pipelineState, somaticRunMetadata);
            fail("An exception should have been thrown");
        } catch (RuntimeException e) {
            verify(sbpRestApi, times(2)).updateRunResult(any(), status.capture());
        }
        assertThat(status.getValue().status()).isEqualTo(SbpSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().result()).contains(FAILED);
    }

    @Test
    public void shouldTransferToArchive() {
        victim.complete(pipelineState, somaticRunMetadata);
        verify(googleArchiver).transfer(somaticRunMetadata);
    }

    @Test
    public void setsRunToFailedAndRethrowsIfArchiveFails() {
        doThrow(new RuntimeException()).when(googleArchiver).transfer(any());
        try {
            victim.complete(pipelineState, somaticRunMetadata);
            fail("An exception should have been thrown");
        } catch (RuntimeException e) {
            verify(sbpRestApi, times(2)).updateRunResult(any(), status.capture());
        }
        assertThat(status.getValue().status()).isEqualTo(SbpSomaticMetadataApi.FINISHED);
        assertThat(status.getValue().result()).contains(FAILED);
    }

    @Test
    public void setsStatusToProcessingAndResultNullOnStartup() {
        ArgumentCaptor<String> runIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SbpRunResultUpdate> runResultUpdateCaptor = ArgumentCaptor.forClass(SbpRunResultUpdate.class);
        victim.start();
        verify(sbpRestApi).updateRunResult(runIdCaptor.capture(), runResultUpdateCaptor.capture());
        assertThat(runIdCaptor.getValue()).isEqualTo(String.valueOf(RUN_ID));
        SbpRunResultUpdate udpate = runResultUpdateCaptor.getValue();
        assertThat(udpate.result()).isEmpty();
        assertThat(udpate.status()).isEqualTo(SbpSomaticMetadataApi.PROCESSING);
    }
}
