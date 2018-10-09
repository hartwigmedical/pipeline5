package com.hartwig.pipeline.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class GoogleStorageToStreamTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "test").build();
    private static final byte[] BYTES = { 1 };
    private SampleDownload victim;
    private RuntimeBucket runtimeBucket;
    private Bucket bucket;
    private ByteArrayOutputStream bamOutputStream;
    private ByteArrayOutputStream baiOutputStream;
    private InputStream resultBamStream;
    private InputStream resultBaiStream;

    @Before
    public void setUp() throws Exception {
        bamOutputStream = new ByteArrayOutputStream();
        baiOutputStream = new ByteArrayOutputStream();
        victim = new GoogleStorageToStream((sample, bamStream, baiStream) -> {
            resultBamStream = bamStream;
            resultBaiStream = baiStream;
        });
        runtimeBucket = mock(RuntimeBucket.class);
        bucket = mock(Bucket.class);
        when(runtimeBucket.bucket()).thenReturn(bucket);
        when(runtimeBucket.getName()).thenReturn("test");
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateExceptionWhenBucketNotFound() {
        victim.run(SAMPLE, runtimeBucket, StatusCheck.Status.SUCCESS);
    }

    @Test
    public void downloadsResultingBamToSuppliedStream() throws Exception {
        Blob bam = mock(Blob.class);
        Blob bai = mock(Blob.class);
        ReadChannel bamChannel = new TestChannel();
        ReadChannel baiChannel = new TestChannel();
        when(bucket.get(eq("results/test.bam"))).thenReturn(bam);
        when(bucket.get(eq("results/test.bam.bai"))).thenReturn(bai);
        when(bam.reader()).thenReturn(bamChannel);
        when(bai.reader()).thenReturn(baiChannel);
        victim.run(SAMPLE, runtimeBucket, StatusCheck.Status.SUCCESS);
        IOUtils.copy(resultBamStream, bamOutputStream);
        IOUtils.copy(resultBaiStream, baiOutputStream);
        assertThat(bamOutputStream.toByteArray()).isEqualTo(BYTES);
        assertThat(baiOutputStream.toByteArray()).isEqualTo(BYTES);
    }

    static class TestChannel implements ReadChannel {
        int readCount = 0;

        @Override
        public void close() {

        }

        @Override
        public void seek(final long position) throws IOException {

        }

        @Override
        public void setChunkSize(final int chunkSize) {

        }

        @Override
        public RestorableState<ReadChannel> capture() {
            return null;
        }

        @Override
        public int read(final ByteBuffer dst) throws IOException {
            dst.put(BYTES);
            readCount++;
            return readCount > 1 ? -1 : 1;
        }

        @Override
        public boolean isOpen() {
            return true;
        }
    }
}