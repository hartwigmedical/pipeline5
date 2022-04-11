package com.hartwig.pipeline.metadata;

public enum InputMode {
    TUMOR_REFERENCE,
    TUMOR_ONLY,
    REFERENCE_ONLY;

    public boolean runTumor() { return this == TUMOR_REFERENCE || this == TUMOR_ONLY; }
    public boolean runGermline() { return this == TUMOR_REFERENCE || this == REFERENCE_ONLY; }
}