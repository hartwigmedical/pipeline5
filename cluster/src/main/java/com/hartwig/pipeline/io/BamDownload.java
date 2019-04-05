package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.JobResult;

public interface BamDownload {

    void run(Sample sample, RuntimeBucket runtimeBucket, JobResult result);
}