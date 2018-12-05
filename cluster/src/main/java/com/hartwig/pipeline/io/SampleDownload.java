package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.JobResult;

public interface SampleDownload {

    void run(Sample sample, RuntimeBucket runtimeBucket, JobResult result);
}