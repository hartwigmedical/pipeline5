package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import com.google.cloud.storage.Blob;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CloudSampleUploadTest {

    private static final String SAMPLE_NAME = "TEST123";
    private static final String SAMPLE_PATH = "samples/" + SAMPLE_NAME + "/";
    private static final String FASTQ_DIR = "/fastq-dir/";
    private static final LaneInput LANE_1 = LaneInput.builder()
            .firstOfPairPath(FASTQ_DIR + "reads1.fastq.gz")
            .secondOfPairPath(FASTQ_DIR + "mates1.fastq.gz")
            .laneNumber("")
            .flowCellId("")
            .build();
    private static final SampleInput SAMPLE_ONE_LANE = SampleInput.builder().name(SAMPLE_NAME).turquoiseSubject(SAMPLE_NAME).addLanes(LANE_1).build();
    private static final String TARGET_PATH = "gs://run/samples/TEST123/";
    private static final String TRANSIENT_FAILURE = "[gcloud storage cp] failed with non-zero exit code [1]";
    private CloudCopy cloudCopy;
    private CloudSampleUpload victim;
    private MockRuntimeBucket mockRuntimeBucket;

    @Before
    public void setUp() throws Exception {
        cloudCopy = mock(CloudCopy.class);
        victim = new CloudSampleUpload(Function.identity(), cloudCopy);
        mockRuntimeBucket = MockRuntimeBucket.of("run");
    }

    @Test
    public void doesNotCopyWhenFileInStorage() {
        mockRuntimeBucket.with(SAMPLE_PATH + LANE_1.firstOfPairPath().replace(FASTQ_DIR, ""), 1)
                .with(SAMPLE_PATH + LANE_1.secondOfPairPath().replace(FASTQ_DIR, ""), 1);
        victim.run(SAMPLE_ONE_LANE, mockRuntimeBucket.getRuntimeBucket());
        verify(cloudCopy, never()).copy(any(), any());
    }

    @Test
    public void doesNotCopyWhenGunzippedInStorage() {
        mockRuntimeBucket.with(
                        "samples/" + SAMPLE_NAME + "/" + LANE_1.firstOfPairPath().replace(FASTQ_DIR, "").replace(".gz", "")
                                + "/", 1)
                .with("samples/" + SAMPLE_NAME + "/" + LANE_1.secondOfPairPath()
                        .replace(FASTQ_DIR, "")
                        .replace(".gz", "") + "/", 1);
        victim.run(SAMPLE_ONE_LANE, mockRuntimeBucket.getRuntimeBucket());
        verify(cloudCopy, never()).copy(any(), any());
    }

    @Test
    public void copiesFilesNotYetInStorage() {
        ArgumentCaptor<String> source = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> target = ArgumentCaptor.forClass(String.class);
        victim.run(SAMPLE_ONE_LANE, mockRuntimeBucket.getRuntimeBucket());
        verify(cloudCopy, times(2)).copy(source.capture(), target.capture());
        assertThat(source.getAllValues()).contains(LANE_1.firstOfPairPath());
        assertThat(target.getAllValues()).contains(TARGET_PATH + LANE_1.firstOfPairPath().replace(FASTQ_DIR, ""));
        assertThat(source.getAllValues()).contains(LANE_1.secondOfPairPath());
        assertThat(target.getAllValues()).contains(TARGET_PATH + LANE_1.secondOfPairPath().replace(FASTQ_DIR, ""));
    }

    @Test
    public void retriesCopyFailingTransiently() {
        victim = new CloudSampleUpload(Function.identity(), cloudCopy, 1);
        doThrow(new RuntimeException(TRANSIENT_FAILURE)).doNothing()
                .when(cloudCopy)
                .copy(eq(LANE_1.firstOfPairPath()), any());
        victim.run(SAMPLE_ONE_LANE, mockRuntimeBucket.getRuntimeBucket());
        verify(cloudCopy, times(2)).copy(eq(LANE_1.firstOfPairPath()), any());
        verify(cloudCopy, times(1)).copy(eq(LANE_1.secondOfPairPath()), any());
    }

    @Test
    public void propagatesFailureWhenRetriesExhausted() {
        victim = new CloudSampleUpload(Function.identity(), cloudCopy, 1);
        doThrow(new RuntimeException(TRANSIENT_FAILURE)).when(cloudCopy).copy(eq(LANE_1.firstOfPairPath()), any());
        assertThatThrownBy(() -> victim.run(SAMPLE_ONE_LANE, mockRuntimeBucket.getRuntimeBucket())).isInstanceOf(RuntimeException.class)
                .hasStackTraceContaining(TRANSIENT_FAILURE);
        verify(cloudCopy, times(2)).copy(eq(LANE_1.firstOfPairPath()), any());
    }

    @Test
    public void doesNotCopyAgainWhenEarlierAttemptLanded() {
        RuntimeBucket bucket = mock(RuntimeBucket.class);
        when(bucket.name()).thenReturn("run");
        when(bucket.get(SAMPLE_PATH + "reads1.fastq.gz")).thenReturn(null).thenReturn(mock(Blob.class));
        doThrow(new RuntimeException(TRANSIENT_FAILURE)).when(cloudCopy).copy(eq(LANE_1.firstOfPairPath()), any());
        doNothing().when(cloudCopy).copy(eq(LANE_1.secondOfPairPath()), any());

        victim = new CloudSampleUpload(Function.identity(), cloudCopy, 1);
        victim.run(SAMPLE_ONE_LANE, bucket);

        verify(cloudCopy, times(1)).copy(eq(LANE_1.firstOfPairPath()), any());
    }
}
