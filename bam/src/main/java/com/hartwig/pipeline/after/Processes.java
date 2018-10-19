package com.hartwig.pipeline.after;

import java.io.IOException;
import java.util.stream.Collectors;

public class Processes {

    public static void run(ProcessBuilder processBuilder) throws IOException, InterruptedException {
        run(processBuilder, true);
    }

    public static void run(ProcessBuilder processBuilder, boolean verbose) throws IOException, InterruptedException {
        ProcessBuilder builder = processBuilder;
        if (verbose) {
            builder = builder.redirectError(ProcessBuilder.Redirect.INHERIT).redirectOutput(ProcessBuilder.Redirect.INHERIT);
        }
        int status = builder.start().waitFor();
        if (status != 0) {
            throw new RuntimeException(String.format("[%s] failed with non-zero exit code [%s]", toString(builder), status));
        }
    }

    public static String toString(final ProcessBuilder processBuilder) {
        return processBuilder.command().stream().collect(Collectors.joining(" "));
    }
}
