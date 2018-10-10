package com.hartwig.pipeline.upload;

import java.io.InputStream;

import com.hartwig.patient.Sample;

public interface BamSink {

    void save(Sample sample, InputStream bam, InputStream bai);
}