package com.hartwig.bcl2fastq.forensics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.util.Collections;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ForensicArchiveTest {

    private static final String CONVERSION = "conversion";
    public static final String LOG = "run.log";
    public static final String TARGET_BUCKET_NAME = "target";
    private Storage storage;
    private ForensicArchive victim;
    private Bucket targetBucket;

    @Before
    public void setUp() {
        storage = mock(Storage.class);
        victim = new ForensicArchive(storage, TARGET_BUCKET_NAME);
        targetBucket = mock(Bucket.class);
        when(storage.get(TARGET_BUCKET_NAME)).thenReturn(targetBucket);
    }

    @Test
    public void onlyStoresLogWithEmptyPaths() {
        victim.store(CONVERSION, Collections.emptyList(), LOG);
        verify(targetBucket, times(1)).create(Mockito.eq(CONVERSION + "/conversion.log"), Mockito.<FileInputStream>any());
    }

    @Test
    public void storesAllPathsInForensicPathFormat() {
        ArgumentCaptor<Storage.CopyRequest> copyRequestArgumentCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        when(storage.copy(copyRequestArgumentCaptor.capture())).thenReturn(mock(CopyWriter.class));
        Blob firstBlob = TestBlobs.blob("path1");
        Blob secondBlob = TestBlobs.blob("path2");
        when(firstBlob.getBucket()).thenReturn("bucket1");
        when(secondBlob.getBucket()).thenReturn("bucket2");
        victim.store(CONVERSION, Lists.newArrayList(firstBlob, secondBlob), LOG);
        Storage.CopyRequest first = copyRequestArgumentCaptor.getAllValues().get(0);
        Storage.CopyRequest second = copyRequestArgumentCaptor.getAllValues().get(1);

        assertThat(first.getSource()).isEqualTo(BlobId.of("bucket1", "path1"));
        assertThat(first.getTarget()).isEqualTo(BlobInfo.newBuilder("target", "conversion/path1").build());
        assertThat(second.getSource()).isEqualTo(BlobId.of("bucket2", "path2"));
        assertThat(second.getTarget()).isEqualTo(BlobInfo.newBuilder("target", "conversion/path2").build());
    }
}