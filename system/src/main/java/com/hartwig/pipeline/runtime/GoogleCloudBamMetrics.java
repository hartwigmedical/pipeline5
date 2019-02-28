package com.hartwig.pipeline.runtime;

import java.io.IOException;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamMetricsPipeline;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudBamMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudBamMetrics.class);

    private final String sampleName;
    private final BamMetricsPipeline bamMetricsPipeline;

    private GoogleCloudBamMetrics(final String sampleName, final FileSystem fileSystem, final String bamDirectory,
            final ReferenceGenome referenceGenome, final Monitor monitor) {
        this.sampleName = sampleName;
        bamMetricsPipeline = BamMetricsPipeline.create(fileSystem, bamDirectory, referenceGenome, monitor);
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
            String runId = args[1];
            String project = args[2];
            String sampleName = args[3];
            String gsBucket = args[4];
            LOGGER.info("Starting bam metrics with version [{}] run id [{}] for project [{}] for sample [{}] in bucket "
                    + "[{}] on Google Dataproc", version, runId, project, sampleName, gsBucket);
            ReferenceGenome referenceGenome = ReferenceGenome.of("");
            new GoogleCloudBamMetrics(sampleName, Hadoop.fileSystem("gs:///"), gsBucket, referenceGenome, Monitor.noop()).execute();
        } catch (IOException e) {
            LOGGER.error("Unable to run Google post-processor. Problems creating the hadoop filesystem, this class can only be run in "
                    + "a Google Dataproc cluster", e);
        }
    }
}
