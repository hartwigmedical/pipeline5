package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.testsupport.TestBlobs;
import com.hartwig.pipeline.transfer.sbp.ContentTypeCorrection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpFileApiUpdateTest {

    public static final String MD5 = "md5";
    private SbpRestApi sbpRestApi;
    private SbpFileApiUpdate victim;
    private Blob blob;
    private ContentTypeCorrection correction;

    @Before
    public void setUp() throws Exception {
        final SbpRun run = mock(SbpRun.class);
        when(run.id()).thenReturn("1");
        final Bucket sourceBucket = mock(Bucket.class);
        sbpRestApi = mock(SbpRestApi.class);
        blob = TestBlobs.blob("set/namespace/blob");
        correction = mock(ContentTypeCorrection.class);
        victim = new SbpFileApiUpdate(correction,
                run,
                sourceBucket,
                sbpRestApi,
                Set.of(new AddDatatype(DataType.AMBER, Folder.root(), format("%s/%s", "namespace", "blob"), "barcode")));
    }

    @Test
    public void filtersOutStagingBlobs() {
        victim.accept(TestBlobs.blob("STAGED"));
        verifyZeroInteractions(sbpRestApi);
    }

    @Test(expected = IllegalStateException.class)
    public void throwsIllegalArgumentOnMissingMD5() {
        when(blob.getMd5()).thenReturn(null);
        victim.accept(blob);
    }

    @Test
    public void postsToFileApiForValidFile() {
        when(blob.getMd5()).thenReturn(MD5);
        when(blob.getSize()).thenReturn(1L);
        mockFileApiResponse();
        ArgumentCaptor<SbpFileMetadata> fileMetadataArgumentCaptor = ArgumentCaptor.forClass(SbpFileMetadata.class);
        victim.accept(blob);
        verify(sbpRestApi).postFile(fileMetadataArgumentCaptor.capture());
        SbpFileMetadata result = fileMetadataArgumentCaptor.getValue();
        assertThat(result.filename()).isEqualTo("blob");
        assertThat(result.directory()).isEqualTo("namespace");
        assertThat(result.hash()).isEqualTo("99de");
        assertThat(result.filesize()).isEqualTo(1);
        assertThat(result.run_id()).isEqualTo(1);
    }

    @Test
    public void appliesContentTypeCorrection() {
        when(blob.getMd5()).thenReturn(MD5);
        when(blob.getSize()).thenReturn(1L);
        mockFileApiResponse();
        victim.accept(blob);
        verify(correction).apply(blob);
    }

    @Test
    public void runsFurtherOperationsForMatchingFiles() {
        when(blob.getMd5()).thenReturn(MD5);
        when(blob.getSize()).thenReturn(1L);
        mockFileApiResponse();
        victim.accept(blob);
        verify(sbpRestApi).linkFileToSample(1, "barcode");
    }

    public void mockFileApiResponse() {
        AddFileApiResponse fileApiResponse = mock(AddFileApiResponse.class);
        when(fileApiResponse.id()).thenReturn(1);
        when(sbpRestApi.postFile(any())).thenReturn(fileApiResponse);
    }
}