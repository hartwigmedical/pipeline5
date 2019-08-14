package com.hartwig.bam.runtime;

import static java.lang.String.format;

import java.io.IOException;

import com.hartwig.bam.HadoopStatusReporter;
import com.hartwig.bam.StatusReporter;
import com.hartwig.bam.after.BamIndexPipeline;
import com.hartwig.bam.runtime.configuration.Configuration;
import com.hartwig.bam.runtime.configuration.PipelineParameters;
import com.hartwig.patient.Sample;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudSortAndIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudSortAndIndex.class);

    private final String sampleName;
    private final BamIndexPipeline bamIndexPipeline;
    private final StatusReporter statusReporter;

    private GoogleCloudSortAndIndex(final String bamDirectory, final String sampleName, final FileSystem fileSystem,
            final StatusReporter statusReporter) {
        this.sampleName = sampleName;
        bamIndexPipeline = BamIndexPipeline.fallback(fileSystem, bamDirectory);
        this.statusReporter = statusReporter;
    }

    private void execute() {
        try {
            LOGGER.info("Starting indexing and sorting with sample [{}]", sampleName);
            bamIndexPipeline.execute(Sample.builder("N/A", sampleName).build(), statusReporter);
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
            String namespace = args[5];
            String jobName = args[6];

            LOGGER.info("Starting sort and index with version [{}] run id [{}] for project [{}] for sample [{}] in bucket "
                    + "[{}] on Google Dataproc", version, runId, project, sampleName, gsBucket);
            FileSystem filesystem = Hadoop.fileSystem("gs:///");
            StatusReporter statusReporter = new HadoopStatusReporter(filesystem, format("/%s/results", namespace), jobName);
            new GoogleCloudSortAndIndex(gsBucket, sampleName, filesystem, statusReporter).execute();
        } catch (IOException e) {
            LOGGER.error("Unable to run Google post-processor. Problems creating the hadoop filesystem, this class can only be run in "
                    + "a Google Dataproc cluster", e);
        }
    }
}
