package com.hartwig.pipeline.storage;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Processes {

    public static void run(final ProcessBuilder processBuilder) throws IOException, InterruptedException {
        run(processBuilder, true);
    }

    public static void run(final ProcessBuilder processBuilder, final boolean verbose) throws IOException, InterruptedException {
        run(processBuilder, verbose, 7, TimeUnit.DAYS);
    }

    public static void run(final ProcessBuilder processBuilder, final boolean verbose, final long timeout, final TimeUnit timeoutUnit)
            throws IOException, InterruptedException {
        ProcessBuilder builder = processBuilder;
        if (verbose) {
            builder = builder.redirectError(ProcessBuilder.Redirect.INHERIT).redirectOutput(ProcessBuilder.Redirect.INHERIT);
        }
        Process process = builder.start();
        if (!process.waitFor(timeout, timeoutUnit)) {
            throw new RuntimeException(String.format("Timeout. [%s] took more than [%s %s] to execute",
                    toString(builder),
                    timeout,
                    timeoutUnit));
        }
        if (process.exitValue() != 0) {
            throw new RuntimeException(String.format("[%s] failed with non-zero exit code [%s]", toString(builder), process.exitValue()));
        }
    }

    public static String toString(final ProcessBuilder processBuilder) {
        return processBuilder.command().stream().collect(Collectors.joining(" "));
    }
}
