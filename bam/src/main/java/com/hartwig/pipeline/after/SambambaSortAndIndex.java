package com.hartwig.pipeline.after;

import java.io.IOException;

import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SambambaSortAndIndex implements SortAndIndexer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SambambaSortAndIndex.class);
    private static final long BYTES_PER_GB = (long) Math.pow(1024, 3);

    @Override
    public void execute(final Sample sample, final String workingDirectory) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("sambamba",
                "sort",
                "-t",
                String.valueOf(Runtime.getRuntime().availableProcessors()),
                "-m",
                (int) (Runtime.getRuntime().maxMemory() / BYTES_PER_GB) + "GB",
                "--tmpdir",
                workingDirectory,
                "-o",
                Bams.name(sample, workingDirectory, Bams.SORTED),
                Bams.name(sample, workingDirectory, Bams.UNSORTED));
        LOGGER.info("Sorting and indexing in one command with sambamba [{}]", Processes.toString(processBuilder));
        Processes.run(processBuilder);
        LOGGER.info("Sorting and indexing complete");
    }
}
