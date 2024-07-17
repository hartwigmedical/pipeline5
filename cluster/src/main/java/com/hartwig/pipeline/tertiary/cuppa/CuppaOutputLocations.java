package com.hartwig.pipeline.tertiary.cuppa;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutputLocations {

    static ImmutableCuppaOutputLocations.Builder builder() {
        return ImmutableCuppaOutputLocations.builder();
    }

    GoogleStorageLocation visData();

    GoogleStorageLocation visPlot();

    GoogleStorageLocation predSumm();
}