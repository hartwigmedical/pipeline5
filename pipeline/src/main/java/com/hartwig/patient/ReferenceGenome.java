package com.hartwig.patient;

import com.hartwig.pipeline.Configuration;

import org.immutables.value.Value;

@Value.Immutable
public interface ReferenceGenome {

    @Value.Parameter
    String path();

    static ReferenceGenome from(Configuration configuration) {
        return ImmutableReferenceGenome.of(configuration.referenceGenomePath());
    }
}
