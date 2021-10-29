package com.hartwig.pipeline.tertiary.cuppa;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutputLocations {

    GoogleStorageLocation conclusionTxt();

    GoogleStorageLocation resultCsv();

    GoogleStorageLocation summaryChartPng();

    GoogleStorageLocation featurePlot();

    static ImmutableCuppaOutputLocations.Builder builder() {
        return ImmutableCuppaOutputLocations.builder();
    }
}