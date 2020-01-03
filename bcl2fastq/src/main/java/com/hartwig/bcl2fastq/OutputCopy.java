package com.hartwig.bcl2fastq;

import java.util.function.Consumer;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.results.Conversion;
import com.hartwig.bcl2fastq.results.ConvertedFastq;
import com.hartwig.bcl2fastq.results.ConvertedSample;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class OutputCopy implements Consumer<Conversion> {

    private final Storage storage;
    private final String outputBucket;
    private final RuntimeBucket runtimeBucket;

    public OutputCopy(final Storage storage, final String outputBucket, final RuntimeBucket runtimeBucket) {
        this.storage = storage;
        this.outputBucket = outputBucket;
        this.runtimeBucket = runtimeBucket;
    }

    @Override
    public void accept(final Conversion conversion) {
        for (ConvertedSample sample : conversion.samples()) {
            for (ConvertedFastq convertedFastq : sample.fastq()) {
                copy(convertedFastq.pathR1(), convertedFastq.outputPathR1());
                copy(convertedFastq.pathR2(), convertedFastq.outputPathR2());
            }
        }
    }

    public void copy(final String sourcePath, final String targetPath) {
        storage.copy(Storage.CopyRequest.of(runtimeBucket.getUnderlyingBucket().getName(),
                sourcePath,
                BlobInfo.newBuilder(outputBucket, targetPath).build())).getResult();
    }
}
