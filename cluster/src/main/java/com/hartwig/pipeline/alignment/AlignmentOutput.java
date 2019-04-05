package com.hartwig.pipeline.alignment;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentOutput {

    enum Type{
        TUMOUR, REFERENCE
    }

    @Value.Parameter
    GoogleStorageLocation finalBamLocation();

    @Value.Parameter
    GoogleStorageLocation recalibratedBamLocation();

    @Value.Parameter
    GoogleStorageLocation metricsLocation();

    @Value.Parameter
    JobResult status();

    @Value.Parameter
    Sample sample();

    static AlignmentOutput of() {
        return null;
    }
}
