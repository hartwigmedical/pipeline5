package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
import com.google.api.services.dataproc.v1beta2.DataprocScopes;
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
import com.hartwig.pipeline.io.CloudBamDownload;
import com.hartwig.pipeline.io.CloudCopy;
import com.hartwig.pipeline.io.CloudSampleUpload;
import com.hartwig.pipeline.io.GSUtil;
import com.hartwig.pipeline.io.GSUtilCloudCopy;
import com.hartwig.pipeline.io.LocalFileSource;
import com.hartwig.pipeline.io.LocalFileTarget;
import com.hartwig.pipeline.io.RCloneCloudCopy;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.S3;
import com.hartwig.pipeline.io.SampleUpload;
import com.hartwig.pipeline.io.sbp.SBPRestApi;
import com.hartwig.pipeline.io.sbp.SBPS3BamDownload;
import com.hartwig.pipeline.io.sbp.SBPS3FileSource;
import com.hartwig.pipeline.io.sbp.SBPS3FileTarget;
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
    private final StaticData knownSnpData;
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

    private Bootstrap(final Storage storage, final StaticData referenceGenomeData, final StaticData knownIndelData,
            final StaticData knownSnpData, final SampleSource sampleSource, final BamDownload bamDownload, final SampleUpload sampleUpload,
            final SparkCluster singleNodeCluster, final SparkCluster cluster, final JarUpload jarUpload,
            final ClusterOptimizer clusterOptimizer, final CostCalculator costCalculator, final BamComposer composer,
            final BamComposer recalibratedBamComposer, final GoogleCredentials credentials) {
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

    private void run(Arguments arguments) throws Exception {
        SampleData sampleData = sampleSource.sample(arguments);
        Sample sample = sampleData.sample();
        PerformanceProfile bamProfile = clusterOptimizer.optimize(sampleData);

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sampleData.sample().name(), arguments);
        try {
            Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.name()), arguments.project(), credentials);
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            knownSnpData.copyInto(runtimeBucket);
            if (!arguments.noUpload()) {
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

            if (!arguments.noDownload()) {
                bamDownload.run(sample, runtimeBucket, JobResult.SUCCESS);
            }
            if (!arguments.noCleanup() && !arguments.noDownload()) {
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

    private static void createAndRunBootstrap(Arguments arguments) throws Exception {
        GoogleCredentials credentials =
                GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
        GSUtil.configure(arguments.verboseCloudSdk(), arguments.cloudSdkTimeoutHours());
        GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath());
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();

        NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());

        StaticData referenceGenomeData =
                new StaticData(storage, arguments.referenceGenomeBucket(), "reference_genome", new ReferenceGenomeAlias());
        StaticData knownIndelsData = new StaticData(storage, arguments.knownIndelsBucket(), "known_indels");
        StaticData knownSnpData = new StaticData(storage, "known_snps", "known_snps");
        CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(arguments.cpuPerGBRatio());
        CostCalculator costCalculator = new CostCalculator(credentials, arguments.region(), Costs.defaultCosts());
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        BamComposer mainBamComposer = new BamComposer(storage, resultsDirectory, 32);
        BamComposer recalibratedBamComposer = new BamComposer(storage, resultsDirectory, 32, "recalibrated");
        GoogleDataprocCluster singleNode = GoogleDataprocCluster.from(credentials, nodeInitialization, "singlenode");
        GoogleDataprocCluster parallelProcessing = GoogleDataprocCluster.from(credentials, nodeInitialization, "spark");
        CloudCopy cloudCopy = arguments.useRclone() ? new RCloneCloudCopy(arguments.rclonePath(),
                arguments.rcloneGcpRemote(),
                arguments.rcloneS3Remote(),
                ProcessBuilder::new) : new GSUtilCloudCopy(arguments.cloudSdkPath());

        final SampleSource sampleSource;
        final BamDownload bamDownload;
        final SampleUpload sampleUpload;
        final AmazonS3 s3;

        if (arguments.sbpApiSampleId().isPresent()) {
            int sbpSampleId = arguments.sbpApiSampleId().get();
            SBPRestApi sbpRestApi = SBPRestApi.newInstance(arguments);
            s3 = S3.newClient(arguments.sblS3Url());

            sampleSource = new SBPS3SampleSource(s3, new SBPSampleReader(sbpRestApi));
            bamDownload = new SBPSampleMetadataPatch(s3,
                    sbpRestApi,
                    sbpSampleId,
                    arguments.useRclone()
                            ? new CloudBamDownload(SBPS3FileTarget::from, resultsDirectory, cloudCopy)
                            : SBPS3BamDownload.from(s3, resultsDirectory, arguments.s3UploadThreads()),
                    resultsDirectory,
                    System::getenv);
            sampleUpload = new CloudSampleUpload(new SBPS3FileSource(), cloudCopy);
        } else {
            // If we don't interact with SBP for data, we assume we interact with local instead.
            s3 = null;
            sampleSource = arguments.noUpload()
                    ? new GoogleStorageSampleSource(storage)
                    : new FileSystemSampleSource(Hadoop.localFilesystem(), arguments.patientDirectory());
            bamDownload = new CloudBamDownload(new LocalFileTarget(), resultsDirectory, cloudCopy);
            sampleUpload = new CloudSampleUpload(new LocalFileSource(), cloudCopy);
        }

        new Bootstrap(storage,
                referenceGenomeData,
                knownIndelsData,
                knownSnpData,
                sampleSource,
                bamDownload,
                sampleUpload,
                singleNode,
                parallelProcessing,
                new GoogleStorageJarUpload(),
                new ClusterOptimizer(ratio, arguments.noPreemptibleVms()),
                costCalculator,
                mainBamComposer,
                recalibratedBamComposer,
                credentials).run(arguments);

        if (s3 != null) {
            s3.shutdown();
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Raw arguments [{}]", Stream.of(args).collect(Collectors.joining(", ")));

        Optional<Arguments> optArguments = BootstrapOptions.from(args);
        if (optArguments.isPresent()) {
            Arguments arguments = optArguments.get();
            LOGGER.info("Arguments used for bootstrap & run: " + arguments);

            try {
                createAndRunBootstrap(arguments);
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
                System.exit(1);
            }

            LOGGER.info("Bootstrap completed successfully");
        }
    }
}
