package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.storage.CloudCopy;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SbpFileTransferTest {
    private CloudCopy cloudCopy;
    private SbpS3 sbpS3;
    private SbpRestApi sbpApi;
    private Bucket sourceBucket;
    private ContentTypeCorrection contentType;
    private SbpFileTransfer victim;

    private SomaticRunMetadata metadata;
    private SbpRun sbpRun;
    private String sbpBucket;
    private Blob blob;

    private String directoryForPost = "top_level_directory/and/the/rest/of_the";
    private String filenameForPost = "file.name";
    private String fullBlobPath = "/" + directoryForPost + "/" + filenameForPost;

    @Before
    public void setup() {
        cloudCopy = mock(CloudCopy.class);
        sbpS3 = mock(SbpS3.class);
        sbpApi = mock(SbpRestApi.class);
        sourceBucket = mock(Bucket.class);
        contentType = mock(ContentTypeCorrection.class);
        sbpRun = mock(SbpRun.class);

        when(sourceBucket.getName()).thenReturn("source_bucket");

        victim = new SbpFileTransfer(cloudCopy, sbpS3, sbpApi, sourceBucket, contentType, Arguments.testDefaults());

        metadata = mock(SomaticRunMetadata.class);
        final String runName = "run_name";
        when(metadata.runName()).thenReturn(runName);
        sbpBucket = "output_bucket";

        @SuppressWarnings("unchecked")
        Page<Blob> blobs = mock(Page.class);
        when(sourceBucket.list(Storage.BlobListOption.prefix(runName + "/"))).thenReturn(blobs);
        blob = mock(Blob.class);
        when(blob.getMd5()).thenReturn("md5");
        when(blob.getName()).thenReturn(fullBlobPath);
        when(blob.getSize()).thenReturn(10L);
        when(blobs.iterateAll()).thenReturn(Collections.singletonList(blob));
        when(sbpRun.id()).thenReturn("123");
    }

    @Test
    public void shouldEnsureBucketExists() {
        victim.publish(metadata, sbpRun, sbpBucket);
        verify(sbpS3).ensureBucketExists(sbpBucket);
    }

    @Test
    public void shouldApplyContentTypeToBlob() {
        victim.publish(metadata, sbpRun, sbpBucket);
        verify(contentType).apply(blob);
    }

    @Test
    public void shouldConsiderFileWithNoMd5AsInvalidAndTakeNoActionOnIt() {
        when(blob.getMd5()).thenReturn(null);
        victim.publish(metadata, sbpRun, sbpBucket);
        verifyNoMoreInteractions(contentType, sbpApi);
    }

    @Test
    public void shouldCopyValidFileToSbpS3() {
        victim.publish(metadata, sbpRun, sbpBucket);

        String sourceUrl = format("%s://%s/%s", "gs", sourceBucket.getName(), fullBlobPath);
        String targetUrl = format("%s://%s/%s", "s3", sbpBucket, fullBlobPath);

        verify(cloudCopy).copy(sourceUrl, targetUrl);
    }

    @Test
    public void shouldSetAclsForValidFileInSbpS3() {
        victim.publish(metadata, sbpRun, sbpBucket);
        verify(sbpS3).setAclsOn(sbpBucket, blob.getName());
    }

    @Test
    public void shouldPostValidFileToSbpApiUsingCorrectPath() {
        victim.publish(metadata, sbpRun, sbpBucket);
        ArgumentCaptor<SbpFileMetadata> metadataCaptor = ArgumentCaptor.forClass(SbpFileMetadata.class);
        verify(sbpApi).postFile(metadataCaptor.capture());

        SbpFileMetadata capturedMetadata = metadataCaptor.getValue();
        assertThat(capturedMetadata.directory()).isEqualTo(directoryForPost);
        assertThat(capturedMetadata.filename()).isEqualTo(filenameForPost);
    }

    @Test
    public void shouldSetDirectoryToEmptyStringInPostIfFileIsInRootOfSourceBucket() {
        when(blob.getName()).thenReturn("/filename");
        victim.publish(metadata, sbpRun, sbpBucket);
        ArgumentCaptor<SbpFileMetadata> metadataCaptor = ArgumentCaptor.forClass(SbpFileMetadata.class);
        verify(sbpApi).postFile(metadataCaptor.capture());

        SbpFileMetadata capturedMetadata = metadataCaptor.getValue();
        assertThat(capturedMetadata.directory()).isEqualTo("");
        assertThat(capturedMetadata.filename()).isEqualTo("filename");
    }

    @Test
    public void shouldChangeFormatOfMd5ToHexEncodedForPost() {
        String md5 = "a68824b4a1cf40437cff58a1b430ad78";
        String hexEncodedMd5 = "6baf3cdb86f86b571fe34e37edc7dfe7c6b56f8df469defc";
        when(blob.getMd5()).thenReturn(md5);
        victim.publish(metadata, sbpRun, sbpBucket);
        ArgumentCaptor<SbpFileMetadata> metadataCaptor = ArgumentCaptor.forClass(SbpFileMetadata.class);
        verify(sbpApi).postFile(metadataCaptor.capture());

        assertThat(metadataCaptor.getValue().hash()).isEqualTo(hexEncodedMd5);
    }

    @Test
    public void shouldDeleteSourceObjectsOnSuccessfulTransfer() {
        victim.publish(metadata, sbpRun, sbpBucket);
        verify(blob, times(1)).delete();
    }

    @Test
    public void shouldNotDeleteSourceBucketsWhenCleanupFalse() {
        victim = new SbpFileTransfer(cloudCopy,
                sbpS3,
                sbpApi,
                sourceBucket,
                contentType,
                Arguments.testDefaultsBuilder().cleanup(false).build());
        victim.publish(metadata, sbpRun, sbpBucket);
        verify(blob, never()).delete();
    }

    @Test
    public void filtersOutStagingCompletionFiles() {
        when(blob.getName()).thenReturn("/" + directoryForPost + "/" + PipelineResults.STAGING_COMPLETE);
        victim.publish(metadata, sbpRun, sbpBucket);
        verifyZeroInteractions(cloudCopy);
    }
}