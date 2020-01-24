package com.hartwig.bcl2fastq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;

public class InputPathTest {

    public static final String FLOWCELL_ID = "flowcell_id";
    private Bucket bucket;

    @Before
    public void setUp() {
        bucket = mock(Bucket.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionIfNoPathFoundForId() {

        Page<Blob> empty = TestBlobs.pageOf();
        when(bucket.list()).thenReturn(empty);
        assertThat(InputPath.resolve(bucket, FLOWCELL_ID)).isEmpty();
    }

    @Test
    public void returnsMostRecentPathMatchingFlowcellId() {
        String latestPath = "20200120_" + FLOWCELL_ID;
        Blob latest = TestBlobs.blob(latestPath + "/");
        when(latest.getCreateTime()).thenReturn(2L);
        Blob older = TestBlobs.blob("20200119_" + FLOWCELL_ID);
        when(older.getCreateTime()).thenReturn(1L);
        Blob differentFlowcell = TestBlobs.blob("20200120_different_flowcell");
        when(differentFlowcell.getCreateTime()).thenReturn(3L);
        Page<Blob> page = TestBlobs.pageOf(latest, older, differentFlowcell);
        when(bucket.list()).thenReturn(page);

        assertThat(InputPath.resolve(bucket, FLOWCELL_ID)).isEqualTo(latestPath);
    }
}