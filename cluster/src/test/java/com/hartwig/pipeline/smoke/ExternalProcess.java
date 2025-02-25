package com.hartwig.pipeline.smoke;

import java.io.IOException;
import java.util.List;

public class ExternalProcess {

    static void run(List<String> command) throws IOException, InterruptedException, Failed {
        Process process = new ProcessBuilder(command)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start();

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            var commandString = String.join(" ", command);
            var errorMessage = String.format("[%s] failed with non-zero exit code [%s]", commandString, exitCode);
            throw new Failed(exitCode, errorMessage);
        }
    }

    public static class Failed extends Exception {
        public final int exitCode;
        public Failed(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }
    }
}
