package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUpload extends SubShellCommand {

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        this(targetLocation, RuntimeFiles.typical());
    }

    public OutputUpload(final GoogleStorageLocation targetLocation, final RuntimeFiles runtimeFiles) {
        super(() -> format("cp %s/%s %s && gsutil -qm rsync -r %s/ gs://%s/%s",
                BashStartupScript.LOCAL_LOG_DIR,
                runtimeFiles.log(),
                        VmDirectories.OUTPUT,
                        VmDirectories.OUTPUT,
                        targetLocation.bucket(),
                        targetLocation.path()));
    }
}