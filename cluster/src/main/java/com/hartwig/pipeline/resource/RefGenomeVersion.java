package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    V37("37", "37", "37", "HG19"),
    V38("38", "38", "38", "HG38");

    private final String resources;
    private final String pipeline;
    private final String protect;
    private final String chord;

    RefGenomeVersion(final String resources, final String pipeline, final String protect, final String chord) {
        this.resources = resources;
        this.pipeline = pipeline;
        this.protect = protect;
        this.chord = chord;
    }

    public String resources() {
        return resources;
    }

    public String pipeline() {
        return pipeline;
    }

    public String protect() {
        return protect;
    }

    public String chord() {
        return chord;
    }
}
