package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class InputDownload implements BashCommand {

    private final GoogleStorageLocation sourceLocation;
    private final String localTargetPath;

    public InputDownload(final GoogleStorageLocation sourceLocation) {
        this(sourceLocation, localPath(sourceLocation));
    }

    public InputDownload(final GoogleStorageLocation sourceLocation, final String localTargetPath) {
        this.sourceLocation = sourceLocation;
        this.localTargetPath = localTargetPath;
    }

    private static String localPath(final GoogleStorageLocation sourceLocation) {
        String[] splitPath = sourceLocation.path().split("/");
        return VmDirectories.INPUT + (sourceLocation.isDirectory() ? "" : "/" + splitPath[splitPath.length - 1]);
    }

    @Override
    public String asBash() {
        return format("gsutil %s -qm cp -r -n gs://%s/%s%s %s%s",
                sourceLocation.billingProject().map(p -> "-u " + p).orElse(""),
                sourceLocation.bucket(),
                sourceLocation.path(),
                sourceLocation.isDirectory() ? "/*" : "",
                localTargetPath,
                sourceLocation.isDirectory() ? "/" : "");
    }

    public String getLocalTargetPath() {
        return localTargetPath;
    }

    String getRemoteSourcePath() {
        return format("gs://%s/%s", sourceLocation.bucket(), sourceLocation.path());
    }
}
