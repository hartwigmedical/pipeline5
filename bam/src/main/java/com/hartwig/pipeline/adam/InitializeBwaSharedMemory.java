package com.hartwig.pipeline.adam;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InitializeBwaSharedMemory implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InitializeBwaSharedMemory.class);
    private static boolean initialized = false;

    static synchronized void run(String indexLocation) {
        try {
            if (!initialized) {
                LOGGER.info("Initializing shared memory for BWA. This should only occur once per executor");
                ProcessBuilder pb = new ProcessBuilder("bwa", "shm", indexLocation);
                pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                int exitCode = pb.start().waitFor();
                if (exitCode == 0) {
                    LOGGER.info("Initialization completed successfully");
                } else {
                    LOGGER.warn("Initialization did not complete. Error code [{}]", exitCode);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            initialized = true;
        }
    }
}
