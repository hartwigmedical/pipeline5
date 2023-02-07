package com.hartwig.pipeline.storage;

import static java.lang.String.format;

import java.io.File;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pdl.SampleInput;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudSampleUpload implements SampleUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleUpload.class);
    private static final String SAMPLE_DIRECTORY = "samples/";

    private final Function<String, String> sourceResolver;
    private final CloudCopy cloudCopy;

    public CloudSampleUpload(final Function<String, String> sourceResolver, final CloudCopy cloudCopy) {
        this.sourceResolver = sourceResolver;
        this.cloudCopy = cloudCopy;
    }

    @Override
    public void run(final SampleInput sample, final RuntimeBucket runtimeBucket) {
        try {
            uploadSample(runtimeBucket, sample);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadSample(final RuntimeBucket runtimeBucket, final SampleInput sample) {
        sample.lanes()
                .stream()
                .flatMap(lane -> Stream.of(lane.firstOfPairPath(), lane.secondOfPairPath()))
                .collect(Collectors.toList())
                .parallelStream()
                .forEach(path -> {
                    try {
                        gsutilCP(sample, runtimeBucket, sourceResolver.apply(path));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void gsutilCP(final SampleInput sample, final RuntimeBucket bucket, final String file) {
        String target = singleSampleFile(sample, file);
        String targetPath = format("gs://%s/%s", bucket.name(), target);
        if (bucket.get(target) != null || bucket.get(target.replaceAll(".gz", "") + "/") != null) {
            LOGGER.info("Fastq [{}] already existed in Google Storage. Skipping upload", targetPath);
        } else {
            LOGGER.info("Uploading fastq [{}] to Google Storage bucket [{}]", file, targetPath);
            cloudCopy.copy(file, targetPath);
        }
    }

    @NotNull
    private static String singleSampleFile(final SampleInput sample, final String file) {
        String filename = new File(file).getName();
        return format(SAMPLE_DIRECTORY + "%s/%s", sample.name(), filename);
    }
}