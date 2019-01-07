package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.NodeInitialization;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.cost.Costs;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.GSUtil;
import com.hartwig.pipeline.io.GSUtilBamDownload;
import com.hartwig.pipeline.io.GSUtilSampleUpload;
import com.hartwig.pipeline.io.LocalFileSource;
import com.hartwig.pipeline.io.LocalFileTarget;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.S3;
import com.hartwig.pipeline.io.SampleUpload;
import com.hartwig.pipeline.io.sbp.SBPRestApi;
import com.hartwig.pipeline.io.sbp.SBPS3BamDownload;
import com.hartwig.pipeline.io.sbp.SBPS3FileSource;
import com.hartwig.pipeline.io.sbp.SBPSampleMetadataPatch;
import com.hartwig.pipeline.io.sbp.SBPSampleReader;
import com.hartwig.pipeline.io.sources.FileSystemSampleSource;
import com.hartwig.pipeline.io.sources.GoogleStorageSampleSource;
import com.hartwig.pipeline.io.sources.SBPS3SampleSource;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.performance.ClusterOptimizer;
import com.hartwig.pipeline.performance.CpuFastQSizeRatio;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.staticdata.ReferenceGenomeAlias;
import com.hartwig.pipeline.staticdata.StaticData;
import com.hartwig.support.hadoop.Hadoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private final Storage storage;
    private final StaticData referenceGenomeData;
    private final StaticData knownIndelData;
    private final SampleSource sampleSource;
    private final BamDownload bamDownload;
    private final SampleUpload sampleUpload;
    private final SparkCluster singleNodeCluster;
    private final SparkCluster parallelProcessingCluster;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final CostCalculator costCalculator;
    private final BamComposer composer;
    private final GoogleCredentials credentials;

    private Bootstrap(final Storage storage, final StaticData referenceGenomeData, final StaticData knownIndelData,
            final SampleSource sampleSource, final BamDownload bamDownload, final SampleUpload sampleUpload,
            final SparkCluster singleNodeCluster, final SparkCluster cluster, final JarUpload jarUpload,
            final ClusterOptimizer clusterOptimizer, final CostCalculator costCalculator, final BamComposer composer,
            final GoogleCredentials credentials) {
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.sampleSource = sampleSource;
        this.bamDownload = bamDownload;
        this.sampleUpload = sampleUpload;
        this.singleNodeCluster = singleNodeCluster;
        this.parallelProcessingCluster = cluster;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.costCalculator = costCalculator;
        this.composer = composer;
        this.credentials = credentials;
    }

    private void run(Arguments arguments) throws Exception {
        SampleData sampleData = sampleSource.sample(arguments);
        Sample sample = sampleData.sample();
        PerformanceProfile bamProfile = clusterOptimizer.optimize(sampleData);

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sampleData.sample().name(), arguments);
        registerShutdownHook(arguments, runtimeBucket);
        try {
            Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.getName()), arguments.project(), credentials);
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            if (!arguments.noUpload()) {
                sampleUpload.run(sample, runtimeBucket);
            }
            JarLocation jarLocation = jarUpload.run(runtimeBucket, arguments);

            runJob(Jobs.gunzip(singleNodeCluster, costCalculator, monitor, jarLocation), arguments, sample, runtimeBucket);
            runJob(Jobs.bam(parallelProcessingCluster, costCalculator, monitor, jarLocation, bamProfile, runtimeBucket, arguments),
                    arguments,
                    sample,
                    runtimeBucket);
            composer.run(sample, runtimeBucket);
            runJob(Jobs.sortAndIndex(singleNodeCluster, costCalculator, monitor, jarLocation, runtimeBucket, arguments, sample),
                    arguments,
                    sample,
                    runtimeBucket);

            if (!arguments.noDownload()) {
                bamDownload.run(sample, runtimeBucket, JobResult.SUCCESS);
            }
        } finally {
            cleanupAll(arguments, runtimeBucket);
            LOGGER.info("Bootstrap completed successfully");
        }
    }

    private void registerShutdownHook(final Arguments arguments, final RuntimeBucket runtimeBucket) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Received shutdown signal, shutting down any running clusters and deleting runtime bucket if not done already");
            cleanupAll(arguments, runtimeBucket);
        }));
    }

    private void cleanupAll(final Arguments arguments, final RuntimeBucket runtimeBucket) {
        if (!arguments.noCleanup() && !arguments.noDownload()) {
            runtimeBucket.cleanup();
        }
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

    public static void main(String[] args) {
        LOGGER.info("Raw arguments [{}]", Stream.of(args).collect(Collectors.joining(", ")));
        BootstrapOptions.from(args).ifPresent(arguments -> {
            try {
                final GoogleCredentials credentials =
                        GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
                GSUtil.configure(arguments.verboseCloudSdk());
                GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath());
                Storage storage =
                        StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();

                NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());

                StaticData referenceGenomeData =
                        new StaticData(storage, arguments.referenceGenomeBucket(), "reference_genome", new ReferenceGenomeAlias());
                StaticData knownIndelsData = new StaticData(storage, arguments.knownIndelsBucket(), "known_indels");
                CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(arguments.cpuPerGBRatio());
                CostCalculator costCalculator = new CostCalculator(credentials, arguments.region(), Costs.defaultCosts());
                ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
                BamComposer composer = new BamComposer(storage, resultsDirectory, 32);
                GoogleDataprocCluster singleNode = new GoogleDataprocCluster(credentials, nodeInitialization, "singlenode");
                GoogleDataprocCluster parallelProcessing = new GoogleDataprocCluster(credentials, nodeInitialization, "spark");
                if (arguments.sbpApiSampleId().isPresent()) {
                    int sbpSampleId = arguments.sbpApiSampleId().get();
                    SBPRestApi sbpRestApi = SBPRestApi.newInstance(arguments);
                    AmazonS3 s3 = S3.newClient(arguments.sblS3Url());
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData,
                            new SBPS3SampleSource(s3, new SBPSampleReader(sbpRestApi)), new SBPSampleMetadataPatch(s3,
                                    sbpRestApi,
                                    sbpSampleId,
                            SBPS3BamDownload.from(s3, resultsDirectory, arguments.s3UploadThreads()),
                            resultsDirectory),
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new SBPS3FileSource()),
                            singleNode,
                            parallelProcessing,
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, arguments.noPreemptibleVms()),
                            costCalculator,
                            composer,
                            credentials).run(arguments);
                    s3.shutdown();
                } else {
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData, arguments.noUpload() ? new GoogleStorageSampleSource(storage)
                                    : new FileSystemSampleSource(Hadoop.localFilesystem(), arguments.patientDirectory()),
                            new GSUtilBamDownload(arguments.cloudSdkPath(), new LocalFileTarget()),
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new LocalFileSource()),
                            singleNode,
                            parallelProcessing,
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, arguments.noPreemptibleVms()),
                            costCalculator,
                            composer,
                            credentials).run(arguments);
                }

            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
                System.exit(1);
            }
            System.exit(0);
        });
    }

}
