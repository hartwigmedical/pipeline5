package com.hartwig.pipeline.io;

import java.util.function.Function;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.JobResult;

import org.jetbrains.annotations.NotNull;

public class GSUtilSampleDownload implements SampleDownload {

    private final String gsdkPath;
    private final Function<Sample, String> targetResolver;

    public GSUtilSampleDownload(final String gsdkPath, final Function<Sample, String> targetResolver) {
        this.gsdkPath = gsdkPath;
        this.targetResolver = targetResolver;
    }

    @Override
    public void run(final Sample sample, final RuntimeBucket runtimeBucket, final JobResult result) {
        try {
            String bamPath = String.format("gs://%s/results/%s.sorted.bam", runtimeBucket.getName(), sample.name());
            String targetBam = targetResolver.apply(sample);
            GSUtil.cp(gsdkPath, bamPath, targetBam);
            GSUtil.cp(gsdkPath, bai(bamPath), bai(targetBam));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private String bai(final String path) {
        return path + ".bai";
    }
}
