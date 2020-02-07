package com.hartwig.bcl2fastq.forensics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ForensicArchiveTest {

    private Storage storage;
    private ForensicArchive victim;

    @Before
    public void setUp() {
        storage = mock(Storage.class);
        victim = new ForensicArchive(storage, "target", "conversion");
    }

    @Test
    public void doesNothingWithEmptyPaths() {
        victim.store();
        verifyZeroInteractions(storage);
    }

    @Test
    public void storesAllPathsInForensicPathFormat() {
        ArgumentCaptor<Storage.CopyRequest> copyRequestArgumentCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        when(storage.copy(copyRequestArgumentCaptor.capture())).thenReturn(mock(CopyWriter.class));
        victim.store("bucket1/path1", "bucket2/path2");
        Storage.CopyRequest first = copyRequestArgumentCaptor.getAllValues().get(0);
        Storage.CopyRequest second = copyRequestArgumentCaptor.getAllValues().get(1);

        assertThat(first.getSource()).isEqualTo(BlobId.of("bucket1", "path1"));
        assertThat(first.getTarget()).isEqualTo(BlobInfo.newBuilder("target", "conversion/path1").build());
        assertThat(second.getSource()).isEqualTo(BlobId.of("bucket2", "path2"));
        assertThat(second.getTarget()).isEqualTo(BlobInfo.newBuilder("target", "conversion/path2").build());
    }
}