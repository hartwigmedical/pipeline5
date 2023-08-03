package com.hartwig.pipeline.execution.vm.command;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class InputDownloadIfBlobExistsCommand implements BashCommand {

    private final InputDownloadCommand download;
    private final GoogleStorageLocation googleStorageLocation;

    public InputDownloadIfBlobExistsCommand(final GoogleStorageLocation googleStorageLocation) {
        this.download = new InputDownloadCommand(googleStorageLocation);
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