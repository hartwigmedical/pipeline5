package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

public interface RunMetadata {

    @Value.Default
    default InputMode mode() {
        return InputMode.SOMATIC;
    }

    String name();

    String bucket();

    String set();

    String barcode();
}