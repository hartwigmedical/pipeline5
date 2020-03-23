package com.hartwig.pipeline.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Function;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CloudSampleUploadTest {

    private static final String SAMPLE_NAME = "TEST123";
    private static final String SAMPLE_PATH = "samples/" + SAMPLE_NAME + "/";
    private static final String FASTQ_DIR = "/fastq-dir/";
    private static final Lane LANE_1 = Lane.builder()
            .firstOfPairPath(FASTQ_DIR + "reads1.fastq.gz")
            .secondOfPairPath(FASTQ_DIR + "mates1.fastq.gz")
            .laneNumber("")
            .directory("")
            .index("")
            .suffix("")
            .name("")
            .flowCellId("")
            .build();
    private static final Lane LANE_2 = Lane.builder()
            .firstOfPairPath(FASTQ_DIR + "reads2.fastq.gz")
            .secondOfPairPath(FASTQ_DIR + "mates2.fastq.gz")
            .laneNumber("")
            .directory("")
            .index("")
            .suffix("")
            .name("")
            .flowCellId("")
            .build();
    private static final Sample SAMPLE_ONE_LANE = Sample.builder("", SAMPLE_NAME).addLanes(LANE_1).build();
    private static final String TARGET_PATH = "gs://run/samples/TEST123/";
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
        mockRuntimeBucket.with("samples/" + SAMPLE_NAME + "/" + LANE_1.firstOfPairPath().replace(FASTQ_DIR, "").replace(".gz", "") + "/", 1)
                .with("samples/" + SAMPLE_NAME + "/" + LANE_1.secondOfPairPath().replace(FASTQ_DIR, "").replace(".gz", "") + "/", 1);
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
}