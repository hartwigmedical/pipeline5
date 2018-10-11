package com.hartwig.pipeline.io;

import com.hartwig.patient.Sample;

public interface SampleDownload {

    void run(Sample sample, RuntimeBucket runtimeBucket, StatusCheck.Status status);
}