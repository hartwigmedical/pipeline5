package com.hartwig.pipeline.upload;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

public interface SampleDownload {

    void run(Sample sample, RuntimeBucket runtimeBucket, StatusCheck.Status status);
}