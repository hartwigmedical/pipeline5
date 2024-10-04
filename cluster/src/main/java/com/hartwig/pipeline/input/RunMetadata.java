package com.hartwig.pipeline.input;

public interface RunMetadata {

    String runName();

    String bucket();

    String set();

    String barcode();

    String stagePrefix();
}