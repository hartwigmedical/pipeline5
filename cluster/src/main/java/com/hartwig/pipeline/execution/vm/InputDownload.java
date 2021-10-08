package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class InputDownload implements BashCommand {

    private final GoogleStorageLocation sourceLocation;
    private final String localTargetPath;
    private final List<String> gsutilOptions;

    public InputDownload(final GoogleStorageLocation sourceLocation) {
        this(sourceLocation, localPath(sourceLocation));
    }

    public InputDownload(final GoogleStorageLocation sourceLocation, final String localTargetPath) {
        this(sourceLocation, localTargetPath, Collections.emptyList());
    }

    private InputDownload(final GoogleStorageLocation sourceLocation, final String localTargetPath, final List<String> gsutilOptions) {
        this.sourceLocation = sourceLocation;
        this.localTargetPath = localTargetPath;
        this.gsutilOptions = new ArrayList<>(gsutilOptions);
    }

    public static InputDownload turbo(final GoogleStorageLocation sourceLocation) {
        return new InputDownload(sourceLocation, localPath(sourceLocation), List.of("'GSUtil:parallel_thread_count=1'",
                "GSUtil:sliced_object_download_max_components=$(nproc)"));
    }

    private static String localPath(final GoogleStorageLocation sourceLocation) {
        String[] splitPath = sourceLocation.path().split("/");
        return VmDirectories.INPUT + (sourceLocation.isDirectory() ? "" : "/" + splitPath[splitPath.length - 1]);
    }

    @Override
    public String asBash() {
        String topLevelArgs = format("%s%s", gsutilOptions.stream().map(o -> "-o " + o).collect(Collectors.joining(" ")),
                sourceLocation.billingProject().map(p -> " -u " + p + " ").orElse("")).trim();
        if (!topLevelArgs.isEmpty()) {
            topLevelArgs = topLevelArgs + " ";
        }
        return format("gsutil %s-qm cp -r -n gs://%s/%s%s %s%s",
                topLevelArgs,
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
