package com.hartwig.pipeline.alignment;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentOutput {

    @Value.Parameter
    GoogleStorageLocation finalBamLocation();

    @Value.Parameter
    GoogleStorageLocation finalBaiLocation();

    @Value.Parameter
    GoogleStorageLocation recalibratedBamLocation();

    @Value.Parameter
    GoogleStorageLocation recalibratedBaiLocation();

    @Value.Parameter
    Sample sample();

    static AlignmentOutput of(GoogleStorageLocation finalBamLocation, GoogleStorageLocation finalBaiLocation,
            GoogleStorageLocation recalibratedBamLocation, GoogleStorageLocation recalibratedBaiLocation, Sample sample) {
        return ImmutableAlignmentOutput.of(finalBamLocation, finalBaiLocation, recalibratedBamLocation, recalibratedBaiLocation, sample);
    }
}
