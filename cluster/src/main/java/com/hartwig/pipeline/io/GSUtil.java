package com.hartwig.pipeline.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static void cp(String gsdkPath, String sourceUrl, String targetUrl, String... metadata)
            throws IOException, InterruptedException {
        List<String> metadataOptions = Stream.of(metadata).flatMap(m -> Stream.of("-h", m)).collect(Collectors.toList());
        List<String> command = new ArrayList<>();
        command.add(gsdkPath + "/gsutil");
        command.addAll(metadataOptions);
        command.add("-m");
        command.add("cp");
        command.add(sourceUrl);
        command.add(targetUrl);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Processes.run(processBuilder, VERBOSE);
    }
}
