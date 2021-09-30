package com.hartwig.pipeline.tertiary.cuppa;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

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

    GoogleStorageLocation conclusionTxt();
    GoogleStorageLocation resultCsv();
    GoogleStorageLocation chartPng();
    GoogleStorageLocation featurePlot();
}
