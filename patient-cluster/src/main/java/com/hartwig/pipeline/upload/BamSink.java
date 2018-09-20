package com.hartwig.pipeline.upload;

import java.io.InputStream;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

public interface BamSink {

    void save(Sample sample, RuntimeBucket runtimeBucket, InputStream stream);
}