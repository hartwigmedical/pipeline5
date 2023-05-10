package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    V37("37", "HG37"),
    V38("38", "HG38");

    private final String numeric;
    private final String alphaNumeric;

    RefGenomeVersion(final String numeric, final String alphaNumeric) {
        this.numeric = numeric;
        this.alphaNumeric = alphaNumeric;
    }

    public String numeric() {
        return numeric;
    }

    public String alphaNumeric() {
        return alphaNumeric;
    }
}
