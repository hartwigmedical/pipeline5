package com.hartwig.pipeline.after;

import java.io.IOException;

import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PicardWGSMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(PicardWGSMetrics.class);
    private static final long BYTES_PER_GB = (long) Math.pow(1024, 3);

    public void execute(final Sample sample, final String workingDirectory) throws IOException, InterruptedException {
        /*my $command = $picard." CollectWgsMetrics TMP_DIR=".$tmp_dir." R=".$genome." INPUT=".$bam." OUTPUT=".$output." MINIMUM_MAPPING_QUALITY="   .$min_map_qual." MINIMUM_BASE_QUALITY=".$min_base_qual." COVERAGE_CAP=".$coverage_cap;*/
        ProcessBuilder processBuilder = new ProcessBuilder("picard",
                "CollectWgsMetrics",
                "-t",
                String.valueOf(Runtime.getRuntime().availableProcessors()),
                "-m",
                (int) (Runtime.getRuntime().maxMemory() / BYTES_PER_GB) + "GB",
                "TMP_DIR=",
                workingDirectory,
                "-o",
                Bams.name(sample, workingDirectory, Bams.SORTED),
                Bams.name(sample, workingDirectory, Bams.UNSORTED));
        LOGGER.info("Running CollectWgsMetrics using picard tools [{}]", Processes.toString(processBuilder));
        Processes.run(processBuilder);
        LOGGER.info("CollectWgsMetrics complete");
    }
}
