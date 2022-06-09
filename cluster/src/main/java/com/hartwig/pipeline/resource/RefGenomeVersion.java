package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    V37("37", "37", "37", "HG19", "37"),
    V38("38", "38", "38", "HG38", "38");

    private final String resources;
    private final String pipeline;
    private final String protect;
    private final String chord;
    private final String rose;

    RefGenomeVersion(final String resources, final String pipeline, final String protect, final String chord, final String rose) {
        this.resources = resources;
        this.pipeline = pipeline;
        this.protect = protect;
        this.chord = chord;
        this.rose = rose;
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

    public String rose() {
        return rose;
    }
}
