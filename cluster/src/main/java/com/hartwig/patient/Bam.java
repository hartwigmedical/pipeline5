package com.hartwig.patient;

import org.immutables.value.Value;

@Value.Immutable
public interface Bam {

    String referenceGenomePath();

    String path();
}
