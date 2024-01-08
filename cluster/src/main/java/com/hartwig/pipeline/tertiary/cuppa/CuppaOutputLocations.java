package com.hartwig.pipeline.tertiary.cuppa;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutputLocations {

    GoogleStorageLocation visData();

    GoogleStorageLocation visPlot();

    GoogleStorageLocation predSumm();

    static ImmutableCuppaOutputLocations.Builder builder() {
        return ImmutableCuppaOutputLocations.builder();
    }
}