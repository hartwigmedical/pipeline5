package com.hartwig.pipeline.execution.vm.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.command.CopyLogToOutputCommand;
import com.hartwig.pipeline.execution.vm.command.unix.SubShellCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUploadCommand extends SubShellCommand {

    public OutputUploadCommand(final GoogleStorageLocation targetLocation) {
        super(() -> copyOutputCommand(targetLocation));
    }

    public OutputUploadCommand(final GoogleStorageLocation targetLocation, final RuntimeFiles runtimeFiles) {
        super(() -> new CopyLogToOutputCommand(runtimeFiles).asBash() + " && " + copyOutputCommand(targetLocation));
    }

    private static String copyOutputCommand(final GoogleStorageLocation targetLocation) {
        return String.format("gsutil -qm rsync -r %s/ gs://%s/%s", VmDirectories.OUTPUT, targetLocation.bucket(), targetLocation.path());
    }
}