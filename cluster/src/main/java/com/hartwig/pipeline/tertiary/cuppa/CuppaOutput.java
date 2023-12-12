package com.hartwig.pipeline.tertiary.cuppa;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CuppaOutput extends StageOutput {
    static ImmutableCuppaOutput.Builder builder() {
        return ImmutableCuppaOutput.builder();
    }

    @Override
    default String name() {
        return Cuppa.NAMESPACE;
    }

    Optional<CuppaOutputLocations> maybeCuppaOutputLocations();

    default CuppaOutputLocations cuppaOutputLocations() {
        return maybeCuppaOutputLocations().orElse(CuppaOutputLocations.builder()
                .conclusionTxt(GoogleStorageLocation.empty())
                .conclusionChart(GoogleStorageLocation.empty())
                .featurePlot(GoogleStorageLocation.empty())
                .resultCsv(GoogleStorageLocation.empty())
                .summaryChartPng(GoogleStorageLocation.empty())
                .build());
    }
}
