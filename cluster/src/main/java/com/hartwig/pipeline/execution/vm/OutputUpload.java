package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.io.GoogleStorageLocation;

public class OutputUpload implements BashCommand {

    public static final String OUTPUT_DIRECTORY = "/data/output";
    private final GoogleStorageLocation targetLocation;

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    @Override
    public String asBash() {
        return format("gsutil -qm cp -r %s/ gs://%s/%s", OUTPUT_DIRECTORY, targetLocation.bucket(), targetLocation.path());
    }
}
