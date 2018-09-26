package com.hartwig.pipeline.upload;

import java.io.IOException;

public class GSUtil {

    public static void auth(String gsdkPath, String keyFile) throws IOException, InterruptedException {
        ProcessBuilder processBuilder =
                new ProcessBuilder(gsdkPath + "/gcloud", "auth", "activate-service-account", String.format("--key-file=%s", keyFile));
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        int exitCode = processBuilder.start().waitFor();
        if (exitCode != 0) {
            throw new RuntimeException(String.format("GCloud auth returned a non-zero exit code of [%s]. Unable to continue", exitCode));
        }
    }
}
