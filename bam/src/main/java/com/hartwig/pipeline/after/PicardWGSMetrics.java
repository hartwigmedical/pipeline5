package com.hartwig.pipeline.after;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PicardWGSMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(PicardWGSMetrics.class);

    private final String picardJarPath;

    PicardWGSMetrics(final String picardJarPath) {
        this.picardJarPath = picardJarPath;
    }


    public void execute(final String tmpDirectory, final String sampleBamPath, final String referenceGenomeFile,
            final String outputWgsMetricsFile) throws IOException, InterruptedException {
        // No parameters available to configure threads and memory?
        //        "-t",
        //                String.valueOf(Runtime.getRuntime().availableProcessors()),
        //                "-m",
        //                (int) (Runtime.getRuntime().maxMemory() / BYTES_PER_GB) + "GB",
        ProcessBuilder processBuilder = new ProcessBuilder("java",
                "-jar",
                picardJarPath,
                "CollectWgsMetrics",
                "TMP_DIR=" + tmpDirectory,
                "REFERENCE_SEQUENCE=" + referenceGenomeFile,
                "INPUT=" + sampleBamPath,
                "OUTPUT=" + outputWgsMetricsFile,
                "MINIMUM_MAPPING_QUALITY=20",
                "MINIMUM_BASE_QUALITY=10",
                "COVERAGE_CAP=250");

        LOGGER.info("Running CollectWgsMetrics using picard tools [{}]", Processes.toString(processBuilder));
        Processes.run(processBuilder);
        LOGGER.info("CollectWgsMetrics complete");
    }
}
