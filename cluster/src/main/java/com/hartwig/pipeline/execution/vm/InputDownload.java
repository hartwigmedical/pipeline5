package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.io.GoogleStorageLocation;

public class InputDownload implements BashCommand {

    private static final String DATA_INPUT = "/data/input/";
    private final GoogleStorageLocation sourceLocation;
    private final String localTargetPath;

    public InputDownload(final GoogleStorageLocation sourceLocation) {
        this.sourceLocation = sourceLocation;
        this.localTargetPath = localPath(sourceLocation);
    }

    private String localPath(final GoogleStorageLocation sourceLocation) {
        String[] splitPath = sourceLocation.path().split("/");
        return DATA_INPUT + splitPath[splitPath.length - 1];
    }

    @Override
    public String asBash() {
        return format("gsutil -qm cp gs://%s/%s %s", sourceLocation.bucket(), sourceLocation.path(), localTargetPath);
    }

    public String getLocalTargetPath() {
        return localTargetPath;
    }
}
