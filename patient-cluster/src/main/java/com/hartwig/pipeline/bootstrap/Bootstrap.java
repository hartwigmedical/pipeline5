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
import com.hartwig.patient.io.PatientReader;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.SampleCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.cost.Costs;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.performance.ClusterOptimizer;
import com.hartwig.pipeline.performance.CpuFastQSizeRatio;
import com.hartwig.pipeline.performance.LocalFastqSize;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.performance.S3FastQSize;
import com.hartwig.pipeline.upload.GSUtil;
import com.hartwig.pipeline.upload.GSUtilSampleDownload;
import com.hartwig.pipeline.upload.GSUtilSampleUpload;
import com.hartwig.pipeline.upload.LocalFileSource;
import com.hartwig.pipeline.upload.LocalFileTarget;
import com.hartwig.pipeline.upload.SBPRestApi;
import com.hartwig.pipeline.upload.SBPS3FileSource;
import com.hartwig.pipeline.upload.SBPS3FileTarget;
import com.hartwig.pipeline.upload.SBPSampleDownload;
import com.hartwig.pipeline.upload.SBPSampleReader;
import com.hartwig.pipeline.upload.SampleDownload;
import com.hartwig.pipeline.upload.SampleUpload;
import com.hartwig.support.hadoop.Hadoop;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String MAIN_CLASS = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    private final Storage storage;
    private final StaticData referenceGenomeData;
    private final StaticData knownIndelData;
    private final SampleSource sampleSource;
    private final SampleDownload sampleDownload;
    private final SampleUpload sampleUpload;
    private final SampleCluster cluster;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final CostCalculator costCalculator;
    private final GoogleCredentials credentials;

    private Bootstrap(final Storage storage, final StaticData referenceGenomeData, final StaticData knownIndelData,
            final SampleSource sampleSource, final SampleDownload sampleDownload, final SampleUpload sampleUpload,
            final SampleCluster cluster, final JarUpload jarUpload, final ClusterOptimizer clusterOptimizer,
            final CostCalculator costCalculator, final GoogleCredentials credentials) {
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.sampleSource = sampleSource;
        this.sampleDownload = sampleDownload;
        this.sampleUpload = sampleUpload;
        this.cluster = cluster;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.costCalculator = costCalculator;
        this.credentials = credentials;
    }

    private void run(Arguments arguments) {
        try {
            long startTime = System.currentTimeMillis();
            Sample sample = sampleSource.sample(arguments);
            RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sample, arguments);
            Monitor monitor = Monitor.stackdriver(Run.of(arguments.version(), runtimeBucket.getName()), arguments.project(), credentials);
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            sampleUpload.run(sample, runtimeBucket);
            JarLocation location = jarUpload.run(runtimeBucket, arguments);
            PerformanceProfile performanceProfile = clusterOptimizer.optimize(sample);

            LOGGER.info("Calculated a cluster of the following size [{}]", performanceProfile);
            LOGGER.info("Approximate costCalculator of this run will be [${}] assuming a 4 hour runtime",
                    costCalculator.calculate(performanceProfile, 4));

            cluster.start(performanceProfile, sample, runtimeBucket, arguments);
            cluster.submit(performanceProfile, SparkJobDefinition.of(MAIN_CLASS, location.uri()), arguments);
            if (!arguments.noDownload()) {
                sampleDownload.run(sample, runtimeBucket);
            }
            if (!arguments.noCleanup() || !arguments.noDownload()) {
                runtimeBucket.cleanup();
            }
            long endTime = System.currentTimeMillis();
            Metrics.record(performanceProfile, endTime - startTime, monitor, costCalculator);
        } catch (Exception e) {
            LOGGER.error(
                    "An unexpected error occurred during bootstrap. See exception for more details. Cluster will still be stopped if running",
                    e);
        } finally {
            try {
                if (!arguments.noCleanup()) {
                    cluster.stop(arguments);
                }
            } catch (IOException e) {
                LOGGER.error("Unable to stop cluster! Check console and stop it by hand if still running", e);
            }
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Raw arguments [{}]", Stream.of(args).collect(Collectors.joining(", ")));
        BootstrapOptions.from(args).ifPresent(arguments -> {
            try {
                final GoogleCredentials credentials =
                        GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
                GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath());
                Storage storage =
                        StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();

                NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());

                StaticData referenceGenomeData = new StaticData(storage, "reference_genome", new ReferenceGenomeAlias());
                StaticData knownIndelsData = new StaticData(storage, "known_indels");
                CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(arguments.cpuPerGBRatio());
                CostCalculator costCalculator = new CostCalculator(credentials, arguments.region(), Costs.defaultCosts());
                if (arguments.sbpApiSampleId().isPresent()) {
                    int sbpSampleId = arguments.sbpApiSampleId().get();
                    SBPRestApi sbpRestApi = SBPRestApi.newInstance(arguments);
                    AmazonS3 s3 = S3.newClient(arguments.sblS3Url());
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData,
                            a -> new SBPSampleReader(sbpRestApi).read(sbpSampleId),
                            new SBPSampleDownload(s3,
                                    sbpRestApi,
                                    sbpSampleId,
                                    new GSUtilSampleDownload(arguments.cloudSdkPath(), new SBPS3FileTarget())),
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new SBPS3FileSource()),
                            new GoogleDataprocCluster(credentials, nodeInitialization),
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, new S3FastQSize(s3), arguments.usePreemptibleVms()),
                            costCalculator,
                            credentials).run(arguments);
                } else {
                    new Bootstrap(storage,
                            referenceGenomeData,
                            knownIndelsData,
                            fromLocalFilesystem(),
                            new GSUtilSampleDownload(arguments.cloudSdkPath(), new LocalFileTarget()),
                            new GSUtilSampleUpload(arguments.cloudSdkPath(), new LocalFileSource()),
                            new GoogleDataprocCluster(credentials, nodeInitialization),
                            new GoogleStorageJarUpload(),
                            new ClusterOptimizer(ratio, new LocalFastqSize(), arguments.usePreemptibleVms()),
                            costCalculator,
                            credentials).run(arguments);
                }

            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
            }
        });
    }

    @NotNull
    private static SampleSource fromLocalFilesystem() {
        return a -> {
            try {
                return PatientReader.fromHDFS(Hadoop.localFilesystem(), a.patientDirectory(), a.patientId()).reference();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
