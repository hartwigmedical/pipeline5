package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.PipelineStatus;

public interface BamDownload {

    void run(Sample sample, RuntimeBucket runtimeBucket, PipelineStatus result);
}