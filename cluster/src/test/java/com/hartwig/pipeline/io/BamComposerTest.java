package com.hartwig.pipeline.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BamComposerTest {

    private static final String SAMPLE = "COLO829T";
    private static final String RUNTIME = "runtime";
    private static final String HEADER = ResultsDirectory.defaultDirectory().path("COLO829T.bam_head");
    private Storage storage;
    private RuntimeBucket runtime;
    private Page<Blob> page;
    private BamComposer victim;

    @Before
    public void setUp() throws Exception {
        storage = mock(Storage.class);
        runtime = mock(RuntimeBucket.class);
        when(runtime.getName()).thenReturn(RUNTIME);
        final Bucket bucket = mock(Bucket.class);
        when(runtime.bucket()).thenReturn(bucket);
        //noinspection unchecked
        page = mock(Page.class);
        when(storage.list(eq(RUNTIME), any())).thenReturn(page);
        victim = new BamComposer(storage, ResultsDirectory.defaultDirectory(), 3);
    }

    @Test
    public void noBlobsInBucketDoesNothing() {
        when(page.iterateAll()).thenReturn(new ArrayList<>());
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(storage, never()).compose(any());
    }

    @Test
    public void appendsAllTailPartsToHead() {
        String head = ResultsDirectory.defaultDirectory().path("COLO829T.bam_head");
        String tailPart1 = part(0);
        String tailPart2 = part(1);
        List<Blob> blobs = Arrays.asList(blobOf(tailPart1), blobOf(tailPart2));
        when(page.iterateAll()).thenReturn(blobs);
        ArgumentCaptor<Storage.ComposeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(Storage.ComposeRequest.class);
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(storage, times(1)).compose(requestArgumentCaptor.capture());
        Storage.ComposeRequest result = requestArgumentCaptor.getValue();
        assertThat(result.getSourceBlobs()).hasSize(3);
        assertThat(result.getSourceBlobs().get(0).getName()).isEqualTo(head);
        assertThat(result.getSourceBlobs().get(1).getName()).isEqualTo(tailPart1);
        assertThat(result.getSourceBlobs().get(2).getName()).isEqualTo(tailPart2);
    }

    @Test
    public void recursivelyComposesInPartitionsToASingleFile() {
        List<String> parts = tenTailParts();
        List<Blob> blobs = parts.stream().map(BamComposerTest::blobOf).collect(Collectors.toList());
        when(page.iterateAll()).thenReturn(blobs);
        ArgumentCaptor<Storage.ComposeRequest> requestArgumentCaptor = ArgumentCaptor.forClass(Storage.ComposeRequest.class);
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(storage, times(7)).compose(requestArgumentCaptor.capture());
        assertThat(requestArgumentCaptor.getAllValues()).hasSize(7);
        assertThat(requestArgumentCaptor.getAllValues().get(0).getSourceBlobs()).hasSize(3);
        assertThat(requestArgumentCaptor.getAllValues().get(1).getSourceBlobs()).hasSize(3);
        assertThat(requestArgumentCaptor.getAllValues().get(2).getSourceBlobs()).hasSize(3);
        assertThat(requestArgumentCaptor.getAllValues().get(3).getSourceBlobs()).hasSize(2);
        assertThat(requestArgumentCaptor.getAllValues().get(4).getSourceBlobs()).hasSize(3);
        assertThat(requestArgumentCaptor.getAllValues().get(5).getSourceBlobs()).hasSize(1);
        assertThat(requestArgumentCaptor.getAllValues().get(6).getSourceBlobs()).hasSize(2);
        assertThat(requestArgumentCaptor.getAllValues().get(0).getSourceBlobs().get(0).getName()).isEqualTo(HEADER);
        assertThat(requestArgumentCaptor.getAllValues().get(6).getTarget().getName()).isEqualTo(ResultsDirectory.defaultDirectory()
                .path("COLO829T.bam"));
    }

    @NotNull
    private String part(int partNum) {
        return String.format(ResultsDirectory.defaultDirectory().path("COLO829T.bam_tail/part-r-%s.bam"),
                new DecimalFormat("000").format(partNum));
    }

    private List<String> tenTailParts() {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            parts.add(part(i));
        }
        return parts;
    }

    @NotNull
    private static Blob blobOf(final String name) {
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(name);
        return blob;
    }
}