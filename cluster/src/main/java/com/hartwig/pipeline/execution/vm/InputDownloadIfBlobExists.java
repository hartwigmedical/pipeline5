package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class InputDownloadIfBlobExists implements BashCommand {

    private final InputDownload download;
    private final GoogleStorageLocation googleStorageLocation;

    public InputDownloadIfBlobExists(final GoogleStorageLocation googleStorageLocation) {
        this.download = new InputDownload(googleStorageLocation);
        this.googleStorageLocation = googleStorageLocation;
    }

    @Override
    public String asBash() {
        return String.format("gsutil  -q stat  gs://%s/%s; if [ $? == 0 ]; then  %s ; fi",
                googleStorageLocation.bucket(),
                googleStorageLocation.path(),
                download.asBash());
    }

    public String getLocalTargetPath() {
        return download.getLocalTargetPath();
    }
}