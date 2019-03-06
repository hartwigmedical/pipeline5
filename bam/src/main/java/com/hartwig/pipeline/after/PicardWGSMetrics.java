package com.hartwig.pipeline.after;

import java.io.IOException;

import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PicardWGSMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(PicardWGSMetrics.class);

    public String execute(final Sample sample, final String workingDirectory, final String bamPath, final String referenceGenomeFastaPath)
            throws IOException, InterruptedException {
        // No parameters available to configure threads and memory?
        //        "-t",
        //                String.valueOf(Runtime.getRuntime().availableProcessors()),
        //                "-m",
        //                (int) (Runtime.getRuntime().maxMemory() / BYTES_PER_GB) + "GB",
        String outputFile = workingDirectory + "/" + sample.name() + ".wgsmetrics";
        ProcessBuilder processBuilder = new ProcessBuilder("picard ",
                "CollectWgsMetrics",
                "-TMP_DIR",
                workingDirectory,
                "-R",
                referenceGenomeFastaPath,
                "-INPUT",
                bamPath,
                "-OUTPUT",
                outputFile,
                "-MINIMUM_MAPPING_QUALITY",
                "20",
                "-MINIMUM_BASE_QUALITY",
                "10",
                "-COVERAGE_CAP",
                "250");

        LOGGER.info("Running CollectWgsMetrics using picard tools [{}]", Processes.toString(processBuilder));
        Processes.run(processBuilder);
        LOGGER.info("CollectWgsMetrics complete");

        return outputFile;
    }
}
