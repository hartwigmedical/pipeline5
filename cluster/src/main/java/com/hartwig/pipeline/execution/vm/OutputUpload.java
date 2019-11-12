package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUpload extends SubShellCommand {

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        super(() -> format("cp %s %s && gsutil -qm rsync -dr %s gs://%s/%s",
                BashStartupScript.LOG_FILE,
                VmDirectories.OUTPUT,
                VmDirectories.OUTPUT,
                targetLocation.bucket(),
                targetLocation.path()));
    }
}