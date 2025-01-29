package com.hartwig.pipeline.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GcloudStorage {
    private static final boolean VERBOSE = true;

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl) throws IOException, InterruptedException {
        cp(gsdkPath, sourceUrl, targetUrl, null, false);
    }

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject,
            final String... switches) throws IOException, InterruptedException {
        cp(gsdkPath, sourceUrl, targetUrl, userProject, false, switches);
    }

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject,
            final boolean recurse, final String... switches) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(gcloud(gsdkPath));
        command.add("storage");
        if (userProject != null) {
            command.add("--billing-project=" + userProject);
        }
        command.add("--no-user-output-enabled");
        command.add("-q");
        command.addAll(Arrays.asList(switches));
        command.add("cp");
        if (recurse) {
            command.add("-r");
        }
        command.add(sourceUrl);
        command.add(targetUrl);
        execute(command);
    }

    public static void rm(final String gsdkPath, final String path) {
        try {
            execute(List.of(gcloud(gsdkPath), "storage", "-q", "--no-user-output-enabled", "rm", "-r", "gs://" + path));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void execute(List<String> command) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder(command).inheritIO();
        Processes.run(processBuilder, VERBOSE, 2, TimeUnit.HOURS);
    }

    private static String gcloud(final String gsdkPath) {
        return gsdkPath + "/gcloud";
    }
}
