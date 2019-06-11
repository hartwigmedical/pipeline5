package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface PatientMetadata {

    @Value.Parameter
    String sample();

    @Value.Parameter
    String setName();

    static PatientMetadata of(String sample, String setName){
        return ImmutablePatientMetadata.of(sample, setName);
    }
}
