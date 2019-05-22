package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface PatientMetadata {

    @Value.Parameter
    String setName();

    static PatientMetadata of(String setName){
        return ImmutablePatientMetadata.of(setName);
    }
}
