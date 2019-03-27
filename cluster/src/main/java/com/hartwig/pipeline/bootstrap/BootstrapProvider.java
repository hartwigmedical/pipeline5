package com.hartwig.pipeline.bootstrap;

import com.amazonaws.services.s3.AmazonS3;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.NodeInitialization;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.cost.Costs;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.BamDownload;
import com.hartwig.pipeline.io.CloudBamDownload;
import com.hartwig.pipeline.io.CloudCopy;
import com.hartwig.pipeline.io.CloudSampleUpload;
import com.hartwig.pipeline.io.GSUtilCloudCopy;
import com.hartwig.pipeline.io.LocalFileSource;
import com.hartwig.pipeline.io.LocalFileTarget;
import com.hartwig.pipeline.io.RCloneCloudCopy;
import com.hartwig.pipeline.io.ResultsDirectory;
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
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.performance.ClusterOptimizer;
import com.hartwig.pipeline.performance.CpuFastQSizeRatio;
import com.hartwig.pipeline.resource.Resources;
import com.hartwig.support.hadoop.Hadoop;

abstract class BootstrapProvider {

    private static final int PERFECT_RATIO = 4;
    private final CredentialProvider credentialProvider;
    private final Arguments arguments;

    BootstrapProvider(final CredentialProvider credentials, final Arguments arguments) {
        this.credentialProvider = credentials;
        this.arguments = arguments;
    }

    protected Arguments getArguments() {
        return arguments;
    }

    abstract Bootstrap wireUp(GoogleCredentials credentials, Storage storage, Resources resources, ClusterOptimizer optimizer,
            CostCalculator costCalculator, GoogleDataprocCluster singleNode, GoogleDataprocCluster spark, BamComposer defaultComposer,
            BamComposer recalibratedComposer, ResultsDirectory resultsDirectory) throws Exception;

    Bootstrap get() throws Exception {
        GoogleCredentials credentials = credentialProvider.get();
        Storage storage = createStorage(arguments, credentials);
        NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());
        CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(PERFECT_RATIO);
        CostCalculator costCalculator = new CostCalculator(credentials, arguments.region(), Costs.defaultCosts());
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        BamComposer defaultComposer = new BamComposer(storage, resultsDirectory, 32);
        BamComposer recalibratedComposer = new BamComposer(storage, resultsDirectory, 32, "recalibrated");
        GoogleDataprocCluster singleNode = GoogleDataprocCluster.from(credentials, nodeInitialization, "singlenode");
        GoogleDataprocCluster spark = GoogleDataprocCluster.from(credentials, nodeInitialization, "spark");
        Resources resources = Resources.from(storage, arguments);
        ClusterOptimizer optimizer = new ClusterOptimizer(ratio, !arguments.noPreemptibleVms());

        return wireUp(credentials,
                storage,
                resources,
                optimizer,
                costCalculator,
                singleNode,
                spark,
                defaultComposer,
                recalibratedComposer,
                ResultsDirectory.defaultDirectory());
    }

    static BootstrapProvider from(Arguments arguments) throws Exception {
        return from(arguments, new CredentialProvider(arguments));
    }

    static BootstrapProvider from(Arguments arguments, CredentialProvider credentialProvider) throws Exception {
        return arguments.sbpApiSampleId().<BootstrapProvider>map(id -> new SBPBootstrapProvider(credentialProvider, arguments, id)).orElse(
                new LocalBootstrapProvider(credentialProvider, arguments));
    }

    static class LocalBootstrapProvider extends BootstrapProvider {

        private LocalBootstrapProvider(final CredentialProvider credentials, final Arguments arguments) {
            super(credentials, arguments);
        }

        @Override
        Bootstrap wireUp(GoogleCredentials credentials, Storage storage, Resources resources, ClusterOptimizer optimizer,
                CostCalculator costCalculator, GoogleDataprocCluster singleNode, GoogleDataprocCluster spark, BamComposer defaultComposer,
                BamComposer recalibratedComposer, ResultsDirectory resultsDirectory) throws Exception {
            SampleSource sampleSource = getArguments().noUpload()
                    ? new GoogleStorageSampleSource(storage)
                    : new FileSystemSampleSource(Hadoop.localFilesystem(), getArguments().sampleDirectory());
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            BamDownload bamDownload = new CloudBamDownload(new LocalFileTarget(), ResultsDirectory.defaultDirectory(), gsUtilCloudCopy);
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy);
            return new Bootstrap(getArguments(),
                    storage,
                    resources.referenceGenome(),
                    resources.knownIndels(),
                    resources.knownSnps(),
                    sampleSource,
                    bamDownload,
                    sampleUpload,
                    singleNode,
                    spark,
                    new GoogleStorageJarUpload(),
                    optimizer,
                    costCalculator,
                    defaultComposer,
                    recalibratedComposer,
                    credentials);
        }
    }

    static class SBPBootstrapProvider extends BootstrapProvider {

        private final int sbpSampleId;

        private SBPBootstrapProvider(final CredentialProvider credentials, final Arguments arguments, final int sbpSampleId) {
            super(credentials, arguments);
            this.sbpSampleId = sbpSampleId;
        }

        @Override
        Bootstrap wireUp(GoogleCredentials credentials, Storage storage, Resources resources, ClusterOptimizer optimizer,
                CostCalculator costCalculator, GoogleDataprocCluster singleNode, GoogleDataprocCluster spark, BamComposer defaultComposer,
                BamComposer recalibratedComposer, ResultsDirectory resultsDirectory) throws Exception {
            SBPRestApi sbpRestApi = SBPRestApi.newInstance(getArguments());
            AmazonS3 s3 = S3.newClient(getArguments().sbpS3Url());
            SampleSource sampleSource = new SBPS3SampleSource(s3, new SBPSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3Remote(),
                    ProcessBuilder::new);
            BamDownload bamDownload = new SBPSampleMetadataPatch(s3,
                    sbpRestApi, sbpSampleId, SBPS3BamDownload.from(s3, resultsDirectory),
                    resultsDirectory,
                    System::getenv);
            SampleUpload sampleUpload = new CloudSampleUpload(new SBPS3FileSource(), cloudCopy);
            return new Bootstrap(getArguments(),
                    storage,
                    resources.referenceGenome(),
                    resources.knownIndels(),
                    resources.knownSnps(),
                    sampleSource,
                    bamDownload,
                    sampleUpload,
                    singleNode,
                    spark,
                    new GoogleStorageJarUpload(),
                    optimizer,
                    costCalculator,
                    defaultComposer,
                    recalibratedComposer,
                    credentials);
        }
    }

    private static Storage createStorage(final Arguments arguments, final GoogleCredentials credentials) {
        return StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();
    }
}
