package com.hartwig.pipeline.io;

import java.io.IOException;

import com.hartwig.pipeline.after.Processes;

public class GSUtil {

    private static boolean VERBOSE = false;

    public static void configure(boolean verbose) {
        VERBOSE = verbose;
    }

    public static void auth(String gsdkPath, String keyFile) throws IOException, InterruptedException {
        ProcessBuilder processBuilder =
                new ProcessBuilder(gsdkPath + "/gcloud", "auth", "activate-service-account", String.format("--key-file=%s", keyFile));
        Processes.run(processBuilder, VERBOSE);
    }

    static void cp(String gsdkPath, String sourceUrl, String targetUrl) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(gsdkPath + "/gsutil", "-m", "cp", sourceUrl, targetUrl);
        Processes.run(processBuilder, VERBOSE);
    }
}
