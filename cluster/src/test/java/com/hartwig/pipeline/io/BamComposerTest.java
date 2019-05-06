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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.ListSelectionModel;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class BamComposerTest {

    private static final String SAMPLE = "COLO829T";
    private static final String RUNTIME = "runtime";
    private static final String NAMESPACE = "namespace/";
    private static final ResultsDirectory RESULTS_DIRECTORY = ResultsDirectory.defaultDirectory();
    private static final String HEADER = RESULTS_DIRECTORY.path("COLO829T.bam_head");

    private Storage storage;
    private RuntimeBucket runtime;
    private Page<Blob> page;
    private BamComposer victim;

    @Before
    public void setUp() {
        storage = mock(Storage.class);
        runtime = mock(RuntimeBucket.class);
        when(runtime.name()).thenReturn(RUNTIME);
        Blob headerBlob = mock(Blob.class);
        when(runtime.get(HEADER)).thenReturn(headerBlob);
        when(headerBlob.getName()).thenReturn(NAMESPACE + HEADER);
        //noinspection unchecked
        page = mock(Page.class);
        when(runtime.list(any())).thenReturn(page);
        victim = new BamComposer(storage, RESULTS_DIRECTORY, 3);
    }

    @Test
    public void noBlobsInBucketDoesNothing() {
        when(page.iterateAll()).thenReturn(new ArrayList<>());
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(storage, never()).compose(any());
    }

    @Test
    public void appendsAllTailPartsToHead() {
        String head = NAMESPACE + RESULTS_DIRECTORY.path("COLO829T.bam_head");
        String tailPart1 = part(0);
        String tailPart2 = part(1);
        List<Blob> blobs = Arrays.asList(blobOf(tailPart1), blobOf(tailPart2));
        when(page.iterateAll()).thenReturn(blobs);
        ArgumentCaptor<List<String>> sourceCaptor = ArgumentCaptor.forClass(List.class);
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(runtime, times(1)).compose(sourceCaptor.capture(), any());
        List<String> result = sourceCaptor.getValue();
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEqualTo(head);
        assertThat(result.get(1)).isEqualTo(tailPart1);
        assertThat(result.get(2)).isEqualTo(tailPart2);
    }

    @Test
    public void recursivelyComposesInPartitionsToASingleFile() {
        List<String> parts = tenTailParts();
        List<Blob> blobs = parts.stream().map(BamComposerTest::blobOf).collect(Collectors.toList());
        when(page.iterateAll()).thenReturn(blobs);
        ArgumentCaptor<List<String>> sourceCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<String> targetCaptor = ArgumentCaptor.forClass(String.class);
        victim.run(Sample.builder("", SAMPLE).build(), runtime);
        verify(runtime, times(7)).compose(sourceCaptor.capture(), targetCaptor.capture());
        assertThat(sourceCaptor.getAllValues()).hasSize(7);
        assertThat(sourceCaptor.getAllValues().get(0)).hasSize(3);
        assertThat(sourceCaptor.getAllValues().get(1)).hasSize(3);
        assertThat(sourceCaptor.getAllValues().get(2)).hasSize(3);
        assertThat(sourceCaptor.getAllValues().get(3)).hasSize(2);
        assertThat(sourceCaptor.getAllValues().get(4)).hasSize(3);
        assertThat(sourceCaptor.getAllValues().get(5)).hasSize(1);
        assertThat(sourceCaptor.getAllValues().get(6)).hasSize(2);
        assertThat(sourceCaptor.getAllValues().get(0).get(0)).isEqualTo(NAMESPACE + HEADER);
        assertThat(targetCaptor.getValue()).isEqualTo(RESULTS_DIRECTORY.path("COLO829T.bam"));
    }

    @Test
    public void optionallyIncludedSuffixInBamName() {
        victim = new BamComposer(storage, RESULTS_DIRECTORY, 3, "suffix");
        String head = RESULTS_DIRECTORY.path("COLO829T.suffix.bam_head");
        Blob headerBlob = mock(Blob.class);
        when(headerBlob.getName()).thenReturn(NAMESPACE + head);
        when(runtime.get(head)).thenReturn(headerBlob);
        String tailPart1 = part(0);
        List<Blob> blobs = Collections.singletonList(blobOf(tailPart1));
        when(page.iterateAll()).thenReturn(blobs);

        ArgumentCaptor<List<String>> sourceCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<String> targetCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> prefixCaptor = ArgumentCaptor.forClass(String.class);

        victim.run(Sample.builder("", SAMPLE).build(), runtime);

        verify(runtime, times(1)).compose(sourceCaptor.capture(), targetCaptor.capture());
        verify(runtime, times(4)).list(prefixCaptor.capture());
        assertThat(prefixCaptor.getAllValues().get(0)).isEqualTo(RESULTS_DIRECTORY.path(
                "COLO829T.suffix.bam_tail/part-r-"));
        assertThat(sourceCaptor.getValue().get(0)).isEqualTo(NAMESPACE + head);
        assertThat(targetCaptor.getValue()).isEqualTo(RESULTS_DIRECTORY.path("COLO829T.suffix.bam"));
    }

    @NotNull
    private String part(int partNum) {
        return String.format(NAMESPACE + RESULTS_DIRECTORY.path("COLO829T.bam_tail/part-r-%s.bam"),
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