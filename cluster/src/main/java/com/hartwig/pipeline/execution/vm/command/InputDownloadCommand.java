package com.hartwig.pipeline.execution.vm.command;

import static java.lang.String.format;

import java.util.Optional;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class InputDownloadCommand implements BashCommand {

    private final GoogleStorageLocation sourceLocation;
    private final String localTargetPath;

    public InputDownloadCommand(final GoogleStorageLocation sourceLocation) {
        this(sourceLocation, localPath(sourceLocation));
    }

    public InputDownloadCommand(final GoogleStorageLocation sourceLocation, final String localTargetPath) {
        this.sourceLocation = sourceLocation;
        this.localTargetPath = localTargetPath;
    }

    private static String localPath(final GoogleStorageLocation sourceLocation) {
        String[] splitPath = sourceLocation.path().split("/");
        return VmDirectories.INPUT + (sourceLocation.isDirectory() ? "" : "/" + splitPath[splitPath.length - 1]);
    }

    @Override
    public String asBash() {
        if (sourceLocation.equals(GoogleStorageLocation.empty())) {
            return "";
        }
        return format(
                "gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) %s-qm cp -r -n gs://%s/%s%s %s%s",
                sourceLocation.billingProject().map(p -> " -u " + p + " ").orElse(""),
                sourceLocation.bucket(),
                sourceLocation.path(),
                sourceLocation.isDirectory() ? "/*" : "",
                localTargetPath,
                sourceLocation.isDirectory() ? "/" : "");
    }

    public String getLocalTargetPath() {
        return localTargetPath;
    }

    public String getRemoteSourcePath() {
        return format("gs://%s/%s", sourceLocation.bucket(), sourceLocation.path());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static InputDownloadCommand initialiseOptionalLocation(final Optional<GoogleStorageLocation> sourceLocation) {
        return new InputDownloadCommand(sourceLocation.orElse(GoogleStorageLocation.empty()));
    }
}
