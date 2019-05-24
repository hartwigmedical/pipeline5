package com.hartwig.pipeline.alignment;

import com.amazonaws.services.s3.AmazonS3;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.dataproc.ClusterOptimizer;
import com.hartwig.pipeline.execution.dataproc.CpuFastQSizeRatio;
import com.hartwig.pipeline.execution.dataproc.GoogleDataproc;
import com.hartwig.pipeline.execution.dataproc.GoogleStorageJarUpload;
import com.hartwig.pipeline.execution.dataproc.NodeInitialization;
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
import com.hartwig.support.hadoop.Hadoop;

public abstract class AlignerProvider {

    private static final int PERFECT_RATIO = 4;
    private final GoogleCredentials credentials;
    private final Storage storage;
    private final Arguments arguments;

    AlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
        this.credentials = credentials;
        this.storage = storage;
        this.arguments = arguments;
    }

    protected Arguments getArguments() {
        return arguments;
    }

    abstract Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
            ClusterOptimizer optimizer, GoogleDataproc dataproc, ResultsDirectory resultsDirectory)
            throws Exception;

    public Aligner get() throws Exception {
        NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());
        CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(PERFECT_RATIO);
        ClusterOptimizer optimizer = new ClusterOptimizer(ratio, arguments.usePreemptibleVms());
        GoogleDataproc dataproc = GoogleDataproc.from(credentials, nodeInitialization, arguments);
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        AlignmentOutputStorage alignmentOutputStorage = new AlignmentOutputStorage(storage, arguments, resultsDirectory);
        return wireUp(credentials, storage, alignmentOutputStorage, optimizer, dataproc, resultsDirectory);
    }

    public static AlignerProvider from(GoogleCredentials credentials, Storage storage, Arguments arguments) throws Exception {
        return arguments.sbpApiSampleId().<AlignerProvider>map(id -> new SBPBootstrapProvider(credentials, storage, arguments, id)).orElse(
                new LocalBootstrapProvider(credentials, storage, arguments));
    }

    static class LocalBootstrapProvider extends AlignerProvider {

        private LocalBootstrapProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ClusterOptimizer optimizer, GoogleDataproc spark, ResultsDirectory resultsDirectory)
                throws Exception {
            SampleSource sampleSource = getArguments().upload()
                    ? new FileSystemSampleSource(Hadoop.localFilesystem(), getArguments().sampleDirectory())
                    : new GoogleStorageSampleSource(storage);
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            BamDownload bamDownload = new CloudBamDownload(new LocalFileTarget(), resultsDirectory, gsUtilCloudCopy);
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy);
            return new Aligner(getArguments(),
                    storage,
                    sampleSource,
                    bamDownload,
                    sampleUpload,
                    spark,
                    new GoogleStorageJarUpload(),
                    optimizer, resultsDirectory,
                    alignmentOutputStorage);
        }
    }

    static class SBPBootstrapProvider extends AlignerProvider {

        private final int sbpSampleId;

        private SBPBootstrapProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
                final int sbpSampleId) {
            super(credentials, storage, arguments);
            this.sbpSampleId = sbpSampleId;
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ClusterOptimizer optimizer, GoogleDataproc dataproc, ResultsDirectory resultsDirectory)
                throws Exception {
            SBPRestApi sbpRestApi = SBPRestApi.newInstance(getArguments());
            AmazonS3 s3 = S3.newClient(getArguments().sbpS3Url());
            SampleSource sampleSource = new SBPS3SampleSource(s3, new SBPSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3Remote(),
                    ProcessBuilder::new);
            BamDownload bamDownload = new SBPSampleMetadataPatch(s3,
                    sbpRestApi,
                    sbpSampleId,
                    SBPS3BamDownload.from(s3, resultsDirectory),
                    resultsDirectory,
                    System::getenv);
            SampleUpload sampleUpload = new CloudSampleUpload(new SBPS3FileSource(), cloudCopy);
            return new Aligner(getArguments(),
                    storage,
                     sampleSource,
                    bamDownload,
                    sampleUpload,
                    dataproc,
                    new GoogleStorageJarUpload(),
                    optimizer, resultsDirectory,
                    alignmentOutputStorage);
        }
    }
}
