package com.hartwig.pipeline;

import java.util.Map;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    enum Flavour {
        GATK4,
        ADAM
    }

    @Value.Default
    default Flavour flavour() {
        return Flavour.ADAM;
    }

    String sparkMaster();

    String patientDirectory();

    String patientName();

    String referenceGenomePath();

    Map<String, String> sparkProperties();

    @Value.Default
    default boolean persistIntermediateResults() {
        return false;
    }

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
