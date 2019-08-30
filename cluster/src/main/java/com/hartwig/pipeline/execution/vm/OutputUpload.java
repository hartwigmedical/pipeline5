package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUpload implements BashCommand {

    public static final String OUTPUT_DIRECTORY = "/data/output";
    private final GoogleStorageLocation targetLocation;

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    @Override
    public String asBash() {
        return format("gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r %s/ gs://%s/%s",
                OUTPUT_DIRECTORY,
                targetLocation.bucket(),
                targetLocation.path());
    }
}
