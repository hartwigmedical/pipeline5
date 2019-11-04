package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class OutputUpload implements BashCommand {

    private final GoogleStorageLocation targetLocation;

    public OutputUpload(final GoogleStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    @Override
    public String asBash() {
        return new SubShellCommand(() ->
                format("cp %s %s && gsutil -qm cp %s/** gs://%s/%s/",
                        BashStartupScript.LOG_FILE,
                        VmDirectories.OUTPUT,
                        VmDirectories.OUTPUT,
                        targetLocation.bucket(),
                        targetLocation.path())).asBash();
    }
}
