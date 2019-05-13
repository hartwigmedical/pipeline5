package com.hartwig.pipeline.alignment;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.dataproc.*;
import com.hartwig.pipeline.io.*;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.String.format;

public class Aligner {

    public final static String NAMESPACE = "aligner";

    private static final Logger LOGGER = LoggerFactory.getLogger(Aligner.class);

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
    private final AlignmentOutputStorage alignmentOutputStorage;

    Aligner(final Arguments arguments, final Storage storage, final Resource referenceGenomeData, final Resource knownIndelData,
            final Resource knownSnpData, final SampleSource sampleSource, final BamDownload bamDownload, final SampleUpload sampleUpload,
            final SparkExecutor dataproc, final JarUpload jarUpload, final ClusterOptimizer clusterOptimizer,
            final CostCalculator costCalculator, final GoogleCredentials credentials, final ResultsDirectory resultsDirectory,
            final AlignmentOutputStorage alignmentOutputStorage) {
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
        this.alignmentOutputStorage = alignmentOutputStorage;
    }

    public AlignmentOutput run() throws Exception {

        if (!arguments.runAligner()) {
            return alignmentOutputStorage.get(Sample.builder(arguments.sampleId()).build())
                    .orElseThrow(() -> new IllegalArgumentException(
                            format("Unable to find output for sample [%s]. Please run the aligner first by setting -run_aligner to true", arguments.sampleId())));
        }

        SampleData sampleData = sampleSource.sample(arguments);
        Sample sample = sampleData.sample();

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, sampleData.sample().name(), arguments);
        Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.name()), arguments.project(), credentials);
        referenceGenomeData.copyInto(runtimeBucket);
        knownIndelData.copyInto(runtimeBucket);
        knownSnpData.copyInto(runtimeBucket);
        if (arguments.upload()) {
            sampleUpload.run(sample, runtimeBucket);
        }
        JarLocation jarLocation = jarUpload.run(runtimeBucket, arguments);

        runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor), SparkJobDefinition.gunzip(jarLocation, runtimeBucket), runtimeBucket);
        runJob(Jobs.statusCheckGoogleStorage(dataproc, costCalculator, monitor, resultsDirectory),
                SparkJobDefinition.bamCreation(jarLocation,
                        arguments,
                        runtimeBucket,
                        clusterOptimizer.optimize(sampleData),
                        resultsDirectory),
                runtimeBucket);

        compose(sample, runtimeBucket);
        compose(sample, runtimeBucket, BamCreationPipeline.RECALIBRATED_SUFFIX);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<?> sortIndexBamFuture = executorService.submit(() -> runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor),
                SparkJobDefinition.sortAndIndex(jarLocation, arguments, runtimeBucket, sample, resultsDirectory),
                runtimeBucket));

        Future<?> sortIndexRecalibratedBamFuture =
                executorService.submit(() -> runJob(Jobs.noStatusCheck(dataproc, costCalculator, monitor),
                        SparkJobDefinition.sortAndIndexRecalibrated(jarLocation, arguments, runtimeBucket, sample, resultsDirectory),
                        runtimeBucket));

        sortIndexBamFuture.get();
        sortIndexRecalibratedBamFuture.get();

        AlignmentOutput alignmentOutput =
                alignmentOutputStorage.get(sample).orElseThrow(() -> new RuntimeException("No results found in Google Storage for sample"));

        if (arguments.download()) {
            bamDownload.run(sample, runtimeBucket, JobStatus.SUCCESS);
        }
        return alignmentOutput;
    }

    private void compose(final Sample sample, final RuntimeBucket runtimeBucket) {
        compose(sample, runtimeBucket, "");
    }

    private void compose(final Sample sample, final RuntimeBucket runtimeBucket, final String suffix) {
        new BamComposer(storage, resultsDirectory, 32, suffix).run(sample, runtimeBucket);
    }

    private void runJob(SparkExecutor executor, SparkJobDefinition jobDefinition, RuntimeBucket runtimeBucket) {
        JobStatus result = executor.submit(runtimeBucket, jobDefinition);
        if (result.equals(JobStatus.FAILED)) {
            throw new RuntimeException(format(
                    "Job [%s] reported status failed. Check prior error messages or job logs on Google dataproc",
                    jobDefinition.name()));
        } else {
            LOGGER.info("Job [{}] completed successfully", jobDefinition.name());
        }
    }
}