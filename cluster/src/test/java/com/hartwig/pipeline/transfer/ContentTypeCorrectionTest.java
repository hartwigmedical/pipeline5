package com.hartwig.pipeline.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ContentTypeCorrectionTest {

    private ContentTypeCorrection victim;

    @Before
    public void setUp() throws Exception {
        victim = new ContentTypeCorrection();
    }

    @Test
    public void doesNotApplyCorrectionToFilesWhichDoNotBreakRclone() {
        Blob blob = TestBlobs.blob("test.json");
        victim.apply(blob);
        verify(blob, never()).toBuilder();
    }

    @Test
    public void doesNotApplyCorrectionIfContentTypeAlreadyExists() {
        Blob blob = TestBlobs.blob("test.txt");
        when(blob.getContentType()).thenReturn("text/plain");
        victim.apply(blob);
        verify(blob, never()).toBuilder();
    }

    @Test
    public void appliesCorrectionToTextFiles() {
        appliesCorrection("txt");
    }

    @Test
    public void appliesCorrectionToConfFiles() {
        appliesCorrection("conf");
    }

    private void appliesCorrection(final Object extension) {
        Blob blob = TestBlobs.blob(String.format("test.%s", extension));
        Blob updated = mock(Blob.class);
        Blob.Builder builder = mock(Blob.Builder.class);
        when(blob.toBuilder()).thenReturn(builder);
        ArgumentCaptor<String> contentTypeCaptor = ArgumentCaptor.forClass(String.class);
        when(builder.setContentType(contentTypeCaptor.capture())).thenReturn(builder);
        when(builder.build()).thenReturn(updated);
        victim.apply(blob);
        verify(updated, times(1)).update();
        assertThat(contentTypeCaptor.getValue()).isEqualTo("text/plain");
    }

}