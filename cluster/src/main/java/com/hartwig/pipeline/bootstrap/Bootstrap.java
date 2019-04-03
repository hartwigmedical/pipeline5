package com.hartwig.pipeline.bootstrap;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.SparkExecutor;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.ResultsDirectory;
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
    private final SparkExecutor dataproc;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final CostCalculator costCalculator;
    private final GoogleCredentials credentials;
    private final ResultsDirectory resultsDirectory;

    Bootstrap(final Arguments arguments, final Storage storage, final Resource referenceGenomeData, final Resource knownIndelData,
            final Resource knownSnpData, final SampleSource sampleSource, final BamDownload bamDownload, final SampleUpload sampleUpload,
            final SparkExecutor dataproc, final JarUpload jarUpload, final ClusterOptimizer clusterOptimizer,
            final CostCalculator costCalculator, final GoogleCredentials credentials, final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.knownSnpData = knownSnpData;
        this.sampleSource = sampleSource;
        this.bamDownload = bamDownload;
        this.sampleUpload = sampleUpload;
        this.dataproc = dataproc;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.costCalculator = costCalculator;
        this.resultsDirectory = resultsDirectory;
        this.credentials = credentials;
    }

    void run() throws Exception {
        SampleData sampleData = sampleSource.sample(arguments);
        Sample sample = sampleData.sample();

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sampleData.sample().name(), arguments);
        Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.name()), arguments.project(), credentials);
        referenceGenomeData.copyInto(runtimeBucket);
        knownIndelData.copyInto(runtimeBucket);
        knownSnpData.copyInto(runtimeBucket);
        if (arguments.upload()) {
            sampleUpload.run(sample, runtimeBucket);
        }
        JarLocation jarLocation = jarUpload.run(runtimeBucket, arguments);

        runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor),
                SparkJobDefinition.gunzip(jarLocation),
                runtimeBucket);
        runJob(Jobs.statusCheckGoogleStorage(dataproc, costCalculator, monitor),
                SparkJobDefinition.bamCreation(jarLocation, arguments, runtimeBucket, clusterOptimizer.optimize(sampleData)),
                runtimeBucket);

        compose(sample, runtimeBucket);
        compose(sample, runtimeBucket, BamCreationPipeline.RECALIBRATED_SUFFIX);

        runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor),
                SparkJobDefinition.sortAndIndex(jarLocation,
                        arguments,
                        runtimeBucket,
                        sample,
                        resultsDirectory),
                runtimeBucket);

        if (arguments.runBamMetrics()) {
            runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor),
                    SparkJobDefinition.bamMetrics(jarLocation, arguments, runtimeBucket, PerformanceProfile.singleNode(), sample),
                    runtimeBucket);
        } else {
            LOGGER.info("Skipping BAM metrics job!");
        }

        if (arguments.download()) {
            bamDownload.run(sample, runtimeBucket, JobResult.SUCCESS);
        }
        if (arguments.cleanup()) {
            runtimeBucket.cleanup();
        }

    }

    private void compose(final Sample sample, final RuntimeBucket runtimeBucket) {
        compose(sample, runtimeBucket, "");
    }

    private void compose(final Sample sample, final RuntimeBucket runtimeBucket, final String suffix) {
        new BamComposer(storage, resultsDirectory, 32, suffix).run(sample, runtimeBucket);
    }

    private void runJob(SparkExecutor executor, SparkJobDefinition jobDefinition, RuntimeBucket runtimeBucket) {
        JobResult result = executor.submit(runtimeBucket, jobDefinition);
        if (result.equals(JobResult.FAILED)) {
            throw new RuntimeException(String.format(
                    "Job [%s] reported status failed. Check prior error messages or job logs on Google dataproc",
                    jobDefinition.name()));
        } else {
            LOGGER.info("Job [{}] completed successfully", jobDefinition.name());
        }
    }
}
