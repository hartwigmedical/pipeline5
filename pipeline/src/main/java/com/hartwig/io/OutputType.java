package com.hartwig.io;

public enum OutputType {

    UNMAPPED("bam"),
    ALIGNED("bam"),
    REALIGNED("bam"),
    DUPLICATE_MARKED("bam");

    private final String extension;

    OutputType(final String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }
}
