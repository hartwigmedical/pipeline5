package com.hartwig.pipeline.runtime;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamIndexPipeline;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudSortAndIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudSortAndIndex.class);

    private final String sampleName;
    private final BamIndexPipeline bamIndexPipeline;

    private GoogleCloudSortAndIndex(final String bamDirectory, final String sampleName, final FileSystem fileSystem,
            final Monitor monitor) {
        this.sampleName = sampleName;
        bamIndexPipeline = BamIndexPipeline.fallback(fileSystem, bamDirectory, monitor);
    }

    private void execute() {
        try {
            LOGGER.info("Starting indexing and sorting with sample [{}]", sampleName);
            bamIndexPipeline.execute(Sample.builder("N/A", sampleName).build());
            LOGGER.info("Completed indexing and sorting");
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
            LOGGER.info("Starting sort and index with version [{}] run id [{}] for project [{}] for sample [{}] in bucket "
                    + "[{}] on Google Dataproc", version, runId, project, sampleName, gsBucket);
            new GoogleCloudSortAndIndex(gsBucket,
                    sampleName,
                    Hadoop.fileSystem("gs:///"),
                    Monitor.stackdriver(Run.of(runId, version), project)).execute();
        } catch (IOException e) {
            LOGGER.error("Unable to run Google post-processor. Problems creating the hadoop filesystem, this class can only be run in "
                    + "a Google Dataproc cluster", e);
        }
    }
}
