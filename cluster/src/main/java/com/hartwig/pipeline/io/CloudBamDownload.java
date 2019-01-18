package com.hartwig.pipeline.io;

import java.util.function.Function;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.JobResult;

import org.jetbrains.annotations.NotNull;

public class CloudBamDownload implements BamDownload {

    private final Function<Sample, String> targetResolver;
    private final CloudCopy cloudCopy;

    public CloudBamDownload(final Function<Sample, String> targetResolver, final CloudCopy cloudCopy) {
        this.targetResolver = targetResolver;
        this.cloudCopy = cloudCopy;
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final JobResult result) {
        try {
            String bamPath = String.format("gs://%s/results/%s.sorted.bam", runtimeBucket.getName(), sample.name());
            String targetBam = targetResolver.apply(sample);
            cloudCopy.copy(bamPath, targetBam);
            cloudCopy.copy(bai(bamPath), bai(targetBam));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private String bai(final String path) {
        return path + ".bai";
    }
}
