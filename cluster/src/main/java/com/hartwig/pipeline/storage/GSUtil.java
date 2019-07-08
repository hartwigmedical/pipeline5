package com.hartwig.pipeline.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.hartwig.pipeline.after.Processes;

public class GSUtil {

    private static boolean VERBOSE = false;
    private static int TIMEOUT_HOURS;

    public static void configure(boolean verbose, int timeoutHours) {
        VERBOSE = verbose;
        TIMEOUT_HOURS = timeoutHours;
    }

    public static void auth(String gsdkPath, String keyFile) throws IOException, InterruptedException {
        ProcessBuilder processBuilder =
                new ProcessBuilder(gsdkPath + "/gcloud", "auth", "activate-service-account", String.format("--key-file=%s", keyFile));
        Processes.run(processBuilder, VERBOSE);
    }

    static void cp(String gsdkPath, String sourceUrl, String targetUrl) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(gsdkPath + "/gsutil");
        if (VERBOSE) {
            command.add("-D");
        }
        command.add("-m");
        command.add("cp");
        command.add(sourceUrl);
        command.add(targetUrl);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Processes.run(processBuilder, VERBOSE, TIMEOUT_HOURS, TimeUnit.HOURS);
    }
}
