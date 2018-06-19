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

    String referencePath();

    Map<String, String> sparkProperties();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
