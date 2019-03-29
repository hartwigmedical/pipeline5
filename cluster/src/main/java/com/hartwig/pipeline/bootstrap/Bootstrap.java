package com.hartwig.pipeline.bootstrap;

import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.SampleUpload;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.performance.ClusterOptimizer;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.resource.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    private final Arguments arguments;
    private final Storage storage;
    private final Resource referenceGenomeData;
    private final Resource knownIndelData;
    private final Resource knownSnpData;
    private final SampleSource sampleSource;
    private final BamDownload bamDownload;
    private final SampleUpload sampleUpload;
    private final SparkCluster singleNodeCluster;
    private final SparkCluster parallelProcessingCluster;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final CostCalculator costCalculator;
    private final BamComposer mainBamComposer;
    private final BamComposer recalibratedBamComposer;
    private final GoogleCredentials credentials;

    Bootstrap(final Arguments arguments, final Storage storage, final Resource referenceGenomeData, final Resource knownIndelData,
            final Resource knownSnpData, final SampleSource sampleSource, final BamDownload bamDownload, final SampleUpload sampleUpload,
            final SparkCluster singleNodeCluster, final SparkCluster cluster, final JarUpload jarUpload,
            final ClusterOptimizer clusterOptimizer, final CostCalculator costCalculator, final BamComposer composer,
            final BamComposer recalibratedBamComposer, final GoogleCredentials credentials) {
        this.arguments = arguments;
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.knownSnpData = knownSnpData;
        this.sampleSource = sampleSource;
        this.bamDownload = bamDownload;
        this.sampleUpload = sampleUpload;
        this.singleNodeCluster = singleNodeCluster;
        this.parallelProcessingCluster = cluster;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.costCalculator = costCalculator;
        this.mainBamComposer = composer;
        this.recalibratedBamComposer = recalibratedBamComposer;
        this.credentials = credentials;
    }

    void run() throws Exception {
        SampleData sampleData = sampleSource.sample(arguments);
        Sample sample = sampleData.sample();
        PerformanceProfile bamProfile = clusterOptimizer.optimize(sampleData);

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sampleData.sample().name(), arguments);
        try {
            Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.name()), arguments.project(), credentials);
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            knownSnpData.copyInto(runtimeBucket);
            if (arguments.upload()) {
                sampleUpload.run(sample, runtimeBucket);
            }
            JarLocation jarLocation = jarUpload.run(runtimeBucket, arguments);

            runJob(Jobs.gunzip(singleNodeCluster, costCalculator, monitor, jarLocation), arguments, sample, runtimeBucket);
            runJob(Jobs.bam(parallelProcessingCluster, costCalculator, monitor, jarLocation, bamProfile, runtimeBucket, arguments),
                    arguments,
                    sample,
                    runtimeBucket);
            mainBamComposer.run(sample, runtimeBucket);
            recalibratedBamComposer.run(sample, runtimeBucket);
            runJob(Jobs.sortAndIndex(singleNodeCluster, costCalculator, monitor, jarLocation, runtimeBucket, arguments, sample),
                    arguments,
                    sample,
                    runtimeBucket);

            if (arguments.runBamMetrics()) {
                runJob(Jobs.bamMetrics(singleNodeCluster, costCalculator, monitor, jarLocation, runtimeBucket, arguments, sample),
                        arguments,
                        sample,
                        runtimeBucket);
            } else {
                LOGGER.info("Skipping BAM metrics job!");
            }

            if (arguments.download()) {
                bamDownload.run(sample, runtimeBucket, JobResult.SUCCESS);
            }
            if (arguments.cleanup() || arguments.download()) {
                runtimeBucket.cleanup();
            }
        } finally {
            cleanupAll(arguments);
        }
    }

    private void cleanupAll(final Arguments arguments) {
        stopCluster(arguments, singleNodeCluster);
        stopCluster(arguments, parallelProcessingCluster);
    }

    private void runJob(final Job job, final Arguments arguments, final Sample sample, final RuntimeBucket runtimeBucket) {
        JobResult result = job.execute(sample, runtimeBucket, arguments);
        if (result.equals(JobResult.FAILED)) {
            throw new RuntimeException(String.format(
                    "Job [%s] reported status failed. Check prior error messages or job logs on Google dataproc",
                    job.getName()));
        } else {
            LOGGER.info("Job [{}] completed successfully", job.getName());
        }
    }

    private void stopCluster(final Arguments arguments, final SparkCluster cluster) {
        try {
            cluster.stop(arguments);
        } catch (IOException e) {
            LOGGER.error("Unable to stop cluster! Check console and stop it by hand if still running", e);
        }
    }
}
