package com.hartwig.pipeline.runtime.configuration;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePatientParameters.class)
@Value.Immutable
public interface PatientParameters {

    String directory();

    String name();

    String referenceGenomePath();

    List<String> knownIndelPaths();

    @Value.Check
    default void check() {
        Preconditions.checkState(!knownIndelPaths().isEmpty(), "known indels should have at least one input");
    }
}
