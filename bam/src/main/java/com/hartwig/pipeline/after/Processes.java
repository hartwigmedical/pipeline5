package com.hartwig.pipeline.after;

import java.io.IOException;
import java.util.stream.Collectors;

public class Processes {

    public static void run(ProcessBuilder processBuilder) throws IOException, InterruptedException {
        int status = processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()
                .waitFor();
        if (status != 0) {
            throw new RuntimeException(String.format("[%s] failed with non-zero exit code [%s]", toString(processBuilder), status));
        }
    }

    public static String toString(final ProcessBuilder processBuilder) {
        return processBuilder.command().stream().collect(Collectors.joining(" "));
    }
}
