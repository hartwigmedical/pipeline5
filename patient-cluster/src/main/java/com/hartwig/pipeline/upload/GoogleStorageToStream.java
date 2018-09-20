package com.hartwig.pipeline.upload;

import static java.lang.String.format;

import java.nio.channels.Channels;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageToStream implements SampleDownload {

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageToStream.class);
    private final BamSink bamSink;

    public GoogleStorageToStream(final BamSink bamSink) {
        this.bamSink = bamSink;
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket) {
        String bamFileName = format("%s.bam", sample.name());
        String bamSource = format("results/%s", bamFileName);
        Blob resultBam = runtimeBucket.bucket().get(bamSource);
        LOGGER.info("Downloading BAM from [{}]", bamSource);
        if (resultBam == null) {
            throw new IllegalStateException(String.format("No result BAM was found for sample [%s] in [gs://%s/results]",
                    sample.name(),
                    runtimeBucket.getName()));
        }
        ReadChannel reader = resultBam.reader();
        bamSink.save(sample, runtimeBucket, Channels.newInputStream(reader));
    }
}