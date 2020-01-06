package com.hartwig.bcl2fastq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.bcl2fastq.conversion.FastqId;
import com.hartwig.bcl2fastq.conversion.ImmutableConversion;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class OutputCopyTest {

    public static final String PATH_TO_R1_FASTQ = "/path/to/r1.fastq";
    public static final String PATH_TO_R2_FASTQ = "/path/to/r2.fastq";
    public static final String OUTPUT_R1_FASTQ = "/output/r1.fastq";
    public static final String OUTPUT_R2_FASTQ = "/output/r2.fastq";
    public static final String SOURCE_BUCKET = "bucket";
    public static final String OUTPUT_BUCKET = "output_bucket";
    private Storage storage;
    private RuntimeBucket runtimeBucket;
    private OutputCopy victim;

    @Before
    public void setUp() {
        storage = mock(Storage.class);
        runtimeBucket = mock(RuntimeBucket.class);
        final Bucket bucket = mock(Bucket.class);
        when(runtimeBucket.getUnderlyingBucket()).thenReturn(bucket);
        when(bucket.getName()).thenReturn(SOURCE_BUCKET);
        victim = new OutputCopy(storage, OUTPUT_BUCKET, runtimeBucket);
    }

    @Test
    public void emptyConversionDoesNothing() {
        victim.accept(conversion().build());
        verifyZeroInteractions(storage);
        verifyZeroInteractions(runtimeBucket);
    }

    @Test
    public void copiesFastqFileToTargetPaths() {
        ArgumentCaptor<Storage.CopyRequest> copyRequestArgumentCaptor = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        CopyWriter copyWriter = mock(CopyWriter.class);
        when(storage.copy(copyRequestArgumentCaptor.capture())).thenReturn(copyWriter);
        victim.accept(conversion().addSamples(ConvertedSample.builder()
                .sample("sample")
                .barcode("barcode")
                .project("project")
                .yieldQ30(0)
                .yield(0)
                .addFastq(ConvertedFastq.builder()
                        .id(FastqId.of(1, "barcode"))
                        .pathR1(PATH_TO_R1_FASTQ)
                        .pathR2(PATH_TO_R2_FASTQ)
                        .outputPathR1(OUTPUT_R1_FASTQ)
                        .outputPathR2(OUTPUT_R2_FASTQ)
                        .sizeR1(1)
                        .sizeR2(1)
                        .md5R1("md5")
                        .md5R2("md5")
                        .yieldQ30(0)
                        .yield(0)
                        .build())
                .build()).build());
        Storage.CopyRequest firstCopyRequest = copyRequestArgumentCaptor.getAllValues().get(0);
        Storage.CopyRequest secondCopyRequest = copyRequestArgumentCaptor.getAllValues().get(1);
        assertThat(firstCopyRequest.getSource()).isEqualTo(BlobId.of(SOURCE_BUCKET, PATH_TO_R1_FASTQ));
        assertThat(firstCopyRequest.getTarget()).isEqualTo(BlobInfo.newBuilder(OUTPUT_BUCKET, OUTPUT_R1_FASTQ).build());
        assertThat(secondCopyRequest.getSource()).isEqualTo(BlobId.of(SOURCE_BUCKET, PATH_TO_R2_FASTQ));
        assertThat(secondCopyRequest.getTarget()).isEqualTo(BlobInfo.newBuilder(OUTPUT_BUCKET, OUTPUT_R2_FASTQ).build());
    }

    @NotNull
    public ImmutableConversion.Builder conversion() {
        return Conversion.builder().flowcell("test").totalReads(0).undeterminedReads(0);
    }
}