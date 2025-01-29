package com.hartwig.pipeline.storage;

import java.io.IOException;

public class GcloudStorageCopy implements CloudCopy {

    private final String gsdkPath;

    public GcloudStorageCopy(final String gsdkPath) {
        this.gsdkPath = gsdkPath;
    }

    @Override
    public void copy(final String from, final String to) {
        try {
            GcloudStorage.cp(gsdkPath, from, to);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}