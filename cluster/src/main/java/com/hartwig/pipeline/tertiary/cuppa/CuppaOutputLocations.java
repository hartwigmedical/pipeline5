package com.hartwig.pipeline.tertiary.cuppa;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutputLocations {

    GoogleStorageLocation conclusionChart();

    GoogleStorageLocation conclusionTxt();

    GoogleStorageLocation resultCsv();

    GoogleStorageLocation summaryChartPng();

    GoogleStorageLocation featurePlot();

    GoogleStorageLocation chartPlot();

    static ImmutableCuppaOutputLocations.Builder builder() {
        return ImmutableCuppaOutputLocations.builder();
    }
}