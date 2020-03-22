package com.hartwig.pipeline.storage;

import static java.lang.String.format;

import java.io.File;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.credentials.CredentialProvider;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudSampleUpload implements SampleUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleUpload.class);
    private static final String SAMPLE_DIRECTORY = "samples/";

    private final Function<String, String> sourceResolver;
    private final CloudCopy cloudCopy;
    private final Arguments arguments;

    public CloudSampleUpload(final Function<String, String> sourceResolver, final CloudCopy cloudCopy, final Arguments arguments) {
        this.sourceResolver = sourceResolver;
        this.cloudCopy = cloudCopy;
        this.arguments = arguments;
    }

    @Override
    public void run(Sample sample, RuntimeBucket runtimeBucket) {
        try {
            if (arguments.uploadFromGcp()) {
                GSUtil.auth(arguments.cloudSdkPath(), arguments.uploadPrivateKeyPath());
            }
            uploadSample(runtimeBucket, sample);
            if (arguments.uploadFromGcp()) {
                CredentialProvider.authorize(arguments);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadSample(final RuntimeBucket runtimeBucket, final Sample sample) {
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

    private void gsutilCP(Sample sample, RuntimeBucket bucket, String file) {
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
    private static String singleSampleFile(final Sample sample, final String file) {
        String filename = new File(file).getName();
        return format(SAMPLE_DIRECTORY + "%s/%s", sample.name(), filename);
    }
}