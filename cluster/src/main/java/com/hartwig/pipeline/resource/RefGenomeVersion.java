package com.hartwig.pipeline.resource;

public enum RefGenomeVersion {
    // TODO SAGE from v2.9 onwards will expect 37/38
    V37("hg19", "37", "37", "37", "37", "HG19"),
    V38("hg38", "38", "38", "38", "38", "HG38");

    private final String sage;
    private final String linx;
    private final String resources;
    private final String pipeline;
    private final String protect;
    private final String chord;

    RefGenomeVersion(final String sage, final String linx, final String resources, final String pipeline, final String protect, final String chord) {
        this.sage = sage;
        this.linx = linx;
        this.resources = resources;
        this.pipeline = pipeline;
        this.protect = protect;
        this.chord = chord;
    }

    public String sage() {
        return sage;
    }

    public String linx() {
        return linx;
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
