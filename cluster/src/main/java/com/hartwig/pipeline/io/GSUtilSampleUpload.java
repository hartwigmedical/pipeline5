package com.hartwig.pipeline.io;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.patient.Sample;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GSUtilSampleUpload implements SampleUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleUpload.class);
    private static final String SAMPLE_DIRECTORY = "samples/";

    private final String gsdkPath;
    private final Function<String, String> sourceResolver;

    public GSUtilSampleUpload(final String gsutilPath, final Function<String, String> sourceResolver) {
        this.gsdkPath = gsutilPath;
        this.sourceResolver = sourceResolver;
    }

    @Override
    public void run(Sample sample, RuntimeBucket runtimeBucket) throws IOException {
        LOGGER.info("Uploading sample [{}] into [{}]", sample.name(), runtimeBucket.bucket().getName());
        uploadSample(runtimeBucket, sample);
        LOGGER.info("Upload complete");
    }

    private void uploadSample(final RuntimeBucket runtimeBucket, final Sample sample) {
        sample.lanes()
                .stream()
                .flatMap(lane -> Stream.of(lane.readsPath(), lane.matesPath()))
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

    private void gsutilCP(Sample sample, RuntimeBucket bucket, String file) throws IOException, InterruptedException {
        String target = singleSampleFile(sample, file);
        if (bucket.bucket().get(target) != null || bucket.bucket().get(target.replaceAll(".gz", "")) != null) {
            LOGGER.info("Fastq [{}] already existed in Google Storage. Skipping upload", target);
        } else {
            LOGGER.info("Uploading fastq [{}] to Google Storage", file);
            GSUtil.cp(gsdkPath, file, format("gs://%s/%s", bucket.getName(), target));
        }
    }

    @NotNull
    private static String singleSampleFile(final Sample sample, final String file) {
        String filename = new File(file).getName();
        return format(SAMPLE_DIRECTORY + "%s/%s", sample.name(), filename);
    }
}
