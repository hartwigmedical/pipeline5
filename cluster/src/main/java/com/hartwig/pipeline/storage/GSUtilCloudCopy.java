package com.hartwig.pipeline.storage;

import java.io.IOException;

public class GSUtilCloudCopy implements CloudCopy {

    private final String gsdkPath;

    public GSUtilCloudCopy(final String gsdkPath) {
        this.gsdkPath = gsdkPath;
    }

    @Override
    public void copy(final String from, final String to) {
        try {
            GSUtil.cp(gsdkPath, from, to);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}