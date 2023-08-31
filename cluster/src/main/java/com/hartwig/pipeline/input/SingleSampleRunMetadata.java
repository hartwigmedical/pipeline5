package com.hartwig.pipeline.input;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSingleSampleRunMetadata.class)
@Value.Immutable
public interface SingleSampleRunMetadata extends RunMetadata {

    enum SampleType {
        TUMOR,
        REFERENCE
    }

    @Value.Default
    default String sampleName() {
        return barcode();
    }

    @Value.Default
    @JsonIgnore
    default long entityId() {
        return -1;
    }

    String barcode();

    SampleType type();

    @Override
    default String runName() {
        return barcode();
    }

    List<String> primaryTumorDoids();

    Optional<LocalDate> samplingDate();

    static ImmutableSingleSampleRunMetadata.Builder builder() {
        return ImmutableSingleSampleRunMetadata.builder();
    }
}
