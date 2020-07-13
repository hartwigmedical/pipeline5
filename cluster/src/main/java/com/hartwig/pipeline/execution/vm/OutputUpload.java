package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUpload extends SubShellCommand {

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        super(() -> copyOutputCommand(targetLocation));
    }

    public OutputUpload(final GoogleStorageLocation targetLocation, final RuntimeFiles runtimeFiles) {
        super(() -> new CopyLogToOutput(runtimeFiles).asBash() + " && " + copyOutputCommand(targetLocation));
    }

    private static String copyOutputCommand(final GoogleStorageLocation targetLocation) {
        return format("gsutil -qm rsync -r %s/ gs://%s/%s", VmDirectories.OUTPUT, targetLocation.bucket(), targetLocation.path());
    }
}