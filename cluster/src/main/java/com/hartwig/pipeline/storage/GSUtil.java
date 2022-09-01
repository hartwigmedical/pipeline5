package com.hartwig.pipeline.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GSUtil {

    private static final boolean VERBOSE = false;
    private static final int TIMEOUT_HOURS = 4;

    public static void auth(final String gsdkPath, final String keyFile) throws IOException, InterruptedException {
        ProcessBuilder processBuilder =
                new ProcessBuilder(gsdkPath + "/gcloud", "auth", "activate-service-account", String.format("--key-file=%s", keyFile));
        Processes.run(processBuilder, VERBOSE);
    }

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl) throws IOException, InterruptedException {
        cp(gsdkPath, sourceUrl, targetUrl, null, false);
    }

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject, final String... switches)
            throws IOException, InterruptedException {
        cp(gsdkPath, sourceUrl, targetUrl, userProject, false, switches);
    }

    public static void cp(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject, final boolean recurse, final String... switches)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(gsutil(gsdkPath));
        if (userProject != null) {
            command.add("-u");
            command.add(userProject);
        }
        command.add("-m");
        command.addAll(Arrays.asList(switches));
        command.add("cp");
        if (recurse) {
            command.add("-r");
        }
        command.add(sourceUrl);
        command.add(targetUrl);
        ProcessBuilder processBuilder = new ProcessBuilder(command).inheritIO();
        Processes.run(processBuilder, VERBOSE, TIMEOUT_HOURS, TimeUnit.HOURS);
    }

    public static void rsync(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject, final boolean recurse)
            throws IOException, InterruptedException {
        rsync(gsdkPath, sourceUrl, targetUrl, userProject, null, recurse);
    }

    public static void rsync(final String gsdkPath, final String sourceUrl, final String targetUrl, final String userProject, final String exclusions, final boolean recurse)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(gsutil(gsdkPath));
        if (userProject != null) {
            command.add("-u");
            command.add(userProject);
        }
        command.add("-qm");
        command.add("rsync");
        if (exclusions != null) {
            command.add("-x");
            command.add(exclusions);
        }
        if (recurse) {
            command.add("-r");
        }
        command.add(sourceUrl);
        command.add(targetUrl);
        ProcessBuilder processBuilder = new ProcessBuilder(command).inheritIO();
        Processes.run(processBuilder, VERBOSE, TIMEOUT_HOURS, TimeUnit.HOURS);
    }

    public static void rm(final String gsdkPath, final String path) {
        List<String> command = new ArrayList<>();
        command.add(gsutil(gsdkPath));
        command.add("-qm");
        command.add("rm");
        command.add("-r");
        command.add("gs://" + path);
        ProcessBuilder processBuilder = new ProcessBuilder(command).inheritIO();
        try {
            Processes.run(processBuilder, VERBOSE, TIMEOUT_HOURS, TimeUnit.HOURS);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String gsutil(final String gsdkPath) {
        return gsdkPath + "/gsutil";
    }
}
