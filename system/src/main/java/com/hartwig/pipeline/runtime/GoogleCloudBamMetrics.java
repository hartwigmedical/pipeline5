package com.hartwig.pipeline.runtime;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamMetricsPipeline;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudBamMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudBamMetrics.class);
    // In google environment we expect the below jar to be present. See also node_init.sh
    private static final String PICARD_JAR_PATH = "/usr/local/bin/picard.jar";

    private final String sampleName;
    private final BamMetricsPipeline bamMetricsPipeline;

    private GoogleCloudBamMetrics(final String sampleName, final FileSystem fileSystem, final String bamDirectory,
            final String refGenomeDirectory, final Monitor monitor) {
        this.sampleName = sampleName;
        this.bamMetricsPipeline = BamMetricsPipeline.create(PICARD_JAR_PATH, fileSystem, bamDirectory, refGenomeDirectory, monitor);
    }

    private void execute() {
        try {
            LOGGER.info("Starting bam metrics for sample [{}]", sampleName);
            bamMetricsPipeline.execute(Sample.builder("N/A", sampleName).build());
            LOGGER.info("Completed bam metrics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            String version = args[0];
            String gsBucket = args[1];
            String sampleName = args[2];

            LOGGER.info("Starting bam metrics with version [{}] for sample [{}] in bucket " + "[{}] on Google Dataproc",
                    version,
                    sampleName,
                    gsBucket);

            new GoogleCloudBamMetrics(sampleName,
                    Hadoop.fileSystem("gs:///"),
                    gsBucket + "/results",
                    gsBucket + "/reference_genome",
                    Monitor.noop()).execute();
        } catch (IOException e) {
            LOGGER.error("Unable to run Google post-processor. Problems creating the hadoop filesystem, this class can only be run in "
                    + "a Google Dataproc cluster", e);
        }
    }
}
