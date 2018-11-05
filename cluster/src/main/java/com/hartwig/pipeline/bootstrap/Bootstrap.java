package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.time.Clock;
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
import com.hartwig.pipeline.cluster.SampleCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.cost.Costs;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.GSUtil;
import com.hartwig.pipeline.io.GSUtilSampleDownload;
import com.hartwig.pipeline.io.GSUtilSampleUpload;
import com.hartwig.pipeline.io.GoogleStorageStatusCheck;
import com.hartwig.pipeline.io.LocalFileSource;
import com.hartwig.pipeline.io.LocalFileTarget;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.S3;
import com.hartwig.pipeline.io.SBPRestApi;
import com.hartwig.pipeline.io.SBPS3FileSource;
import com.hartwig.pipeline.io.SBPS3FileTarget;
import com.hartwig.pipeline.io.SBPSampleDownload;
import com.hartwig.pipeline.io.SampleDownload;
import com.hartwig.pipeline.io.SampleUpload;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.io.sources.FileSystemSampleSource;
import com.hartwig.pipeline.io.sources.GoogleStorageSampleSource;
import com.hartwig.pipeline.io.sources.SBPS3SampleSource;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.metrics.Metrics;
import com.hartwig.pipeline.metrics.MetricsTimeline;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.metrics.Stage;
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
    private final SampleDownload sampleDownload;
    private final StatusCheck statusCheck;
    private final SampleUpload sampleUpload;
    private final SampleCluster singleNodeCluster;
    private final SampleCluster parallelProcessingCluster;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final CostCalculator costCalculator;
    private final BamComposer composer;
    private final GoogleCredentials credentials;

    private Bootstrap(final Storage storage, final StaticData referenceGenomeData, final StaticData knownIndelData,
            final SampleSource sampleSource, final SampleDownload sampleDownload, final StatusCheck statusCheck,
            final SampleUpload sampleUpload, final SampleCluster singleNodeCluster, final SampleCluster cluster, final JarUpload jarUpload,
            final ClusterOptimizer clusterOptimizer, final CostCalculator costCalculator, final BamComposer composer,
            final GoogleCredentials credentials) {
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.sampleSource = sampleSource;
        this.sampleDownload = sampleDownload;
        this.statusCheck = statusCheck;
        this.sampleUpload = sampleUpload;
        this.singleNodeCluster = singleNodeCluster;
        this.parallelProcessingCluster = cluster;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.costCalculator = costCalculator;
        this.composer = composer;
        this.credentials = credentials;
    }

    private void run(Arguments arguments) {
        try {
            RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, arguments.patientId(), arguments);
            SampleData sampleData = sampleSource.sample(arguments, runtimeBucket);
            Sample sample = sampleData.sample();
            PerformanceProfile bamProfile = clusterOptimizer.optimize(sampleData);

            Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.getName()), arguments.project(), credentials);
            MetricsTimeline metricsTimeline = new MetricsTimeline(Clock.systemDefaultZone(), new Metrics(monitor, costCalculator));
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            if (!arguments.noUpload()) {
                sampleUpload.run(sample, runtimeBucket);
            }
            JarLocation location = jarUpload.run(runtimeBucket, arguments);

            PerformanceProfile singleNode = PerformanceProfile.singleNode();
            metricsTimeline.start(Stage.gunzip(singleNode));
            singleNodeCluster.start(singleNode, sample, runtimeBucket, arguments);
            singleNodeCluster.submit(SparkJobDefinition.gunzip(location.uri(), singleNode), arguments);
            singleNodeCluster.stop(arguments);
            metricsTimeline.stop(Stage.gunzip(singleNode));

            LOGGER.info("Calculated a cluster of the following size [{}]", bamProfile);
            LOGGER.info("This cluster will cost approximately [{}] per hour",
                    NumberFormat.getCurrencyInstance().format(costCalculator.calculate(bamProfile, 1)));

            metricsTimeline.start(Stage.bam(bamProfile));
            parallelProcessingCluster.start(bamProfile, sample, runtimeBucket, arguments);
            parallelProcessingCluster.submit(SparkJobDefinition.bamCreation(location.uri(), arguments, runtimeBucket, bamProfile),
                    arguments);
            stopCluster(arguments, parallelProcessingCluster);
            metricsTimeline.stop(Stage.bam(bamProfile));
            StatusCheck.Status status = statusCheck.check(runtimeBucket);
            composer.run(sample, runtimeBucket);

            metricsTimeline.start(Stage.sortIndex(singleNode));
            singleNodeCluster.start(singleNode, sample, runtimeBucket, arguments);
            singleNodeCluster.submit(SparkJobDefinition.sortAndIndex(location.uri(), arguments, runtimeBucket, singleNode,
                    sample,
                    ResultsDirectory.defaultDirectory()), arguments);
            metricsTimeline.stop(Stage.sortIndex(singleNode));
            stopCluster(arguments, singleNodeCluster);

            if (!arguments.noDownload()) {
                sampleDownload.run(sample, runtimeBucket, status);
            }
            if (!arguments.noCleanup() && !arguments.noDownload()) {
                runtimeBucket.cleanup();
            }
        } catch (Exception e) {
            LOGGER.error(
                    "An unexpected error occurred during bootstrap. See exception for more details. Cluster will still be stopped if running",
                    e);
        } finally {
            stopCluster(arguments, singleNodeCluster);
            stopCluster(arguments, parallelProcessingCluster);
        }
    }

    private void stopCluster(final Arguments arguments, final SampleCluster cluster) {
        try {
            if (!arguments.noCleanup()) {
                cluster.stop(arguments);
            }
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
                StaticData knownIndelsData = new StaticData(storage, "known_indels", "known_indels");
                CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(arguments.cpuPerGBRatio());
                CostCalculator costCalculator = new CostCalculator(credentials, arguments.region(), Costs.defaultCosts());
                StatusCheck statusCheck = new GoogleStorageStatusCheck(ResultsDirectory.defaultDirectory());
                BamComposer composer = new BamComposer(storage, ResultsDirectory.defaultDirectory(), 32);
                GoogleDataprocCluster singleNode = new GoogleDataprocCluster(credentials, nodeInitialization, "singlenode");
                GoogleDataprocCluster parallelProcessing = new GoogleDataprocCluster(credentials, nodeInitialization, "spark");
                if (arguments.sbpApiSampleId().isPresent()) {
                    int sbpSampleId = arguments.sbpApiSampleId().get();
                    SBPRestApi sbpRestApi = SBPRestApi.newInstance(arguments);
                    AmazonS3 s3 = S3.newClient(arguments.sblS3Url());
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData,
                            new SBPS3SampleSource(sbpRestApi, s3),
                            new SBPSampleDownload(s3,
                                    sbpRestApi,
                                    sbpSampleId,
                                    new GSUtilSampleDownload(arguments.cloudSdkPath(), new SBPS3FileTarget())),
                            statusCheck,
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new SBPS3FileSource()),
                            singleNode,
                            parallelProcessing,
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, arguments.usePreemptibleVms()),
                            costCalculator,
                            composer,
                            credentials).run(arguments);
                } else {
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData,
                            arguments.noUpload()
                                    ? new GoogleStorageSampleSource()
                                    : new FileSystemSampleSource(Hadoop.localFilesystem(), arguments.patientDirectory()),
                            new GSUtilSampleDownload(arguments.cloudSdkPath(), new LocalFileTarget()),
                            statusCheck,
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new LocalFileSource()),
                            singleNode,
                            parallelProcessing,
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, arguments.usePreemptibleVms()),
                            costCalculator,
                            composer,
                            credentials).run(arguments);
                }

            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
            }
        });
    }

}
