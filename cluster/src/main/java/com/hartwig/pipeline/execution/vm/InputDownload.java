package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.io.GoogleStorageLocation;

public class InputDownload implements BashCommand {

    private final GoogleStorageLocation inputLocation;
    private final String localPath;

    public InputDownload(final GoogleStorageLocation inputLocation) {
        this.inputLocation = inputLocation;
        this.localPath = localPath(inputLocation);
    }

    private String localPath(final GoogleStorageLocation inputLocation) {
        String[] splitPath = inputLocation.path().split("/");
        return "/data/inputs/" + splitPath[splitPath.length - 1];
    }

    @Override
    public String asBash() {
        return format("gsutil -qm cp gs://%s/%s %s", inputLocation.bucket(), inputLocation.path(), localPath);
    }

    public String getLocalPath() {
        return localPath;
    }
}
