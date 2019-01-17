package com.hartwig.pipeline.io;

import java.io.IOException;

public class GSUtilCloudCopy implements CloudCopy {

    private final String gsdkPath;

    public GSUtilCloudCopy(final String gsdkPath) {
        this.gsdkPath = gsdkPath;
    }

    @Override
    public void copy(final String copyId, final String from, final String to, final String... metadata) {
        try {
            String cacheDir = System.getProperty("user.dir") + "/" + copyId;
            GSUtil.cp(gsdkPath, from, to, cacheDir, metadata);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}