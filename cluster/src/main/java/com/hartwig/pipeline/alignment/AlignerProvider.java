package com.hartwig.pipeline.alignment;

import com.amazonaws.services.s3.AmazonS3;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.sample.FileSystemSampleSource;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpS3SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpSampleReader;
import com.hartwig.pipeline.execution.dataproc.ClusterOptimizer;
import com.hartwig.pipeline.execution.dataproc.CpuFastQSizeRatio;
import com.hartwig.pipeline.execution.dataproc.GoogleDataproc;
import com.hartwig.pipeline.execution.dataproc.GoogleStorageJarUpload;
import com.hartwig.pipeline.execution.dataproc.NodeInitialization;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.LocalFileSource;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.S3;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.transfer.SbpS3FileSource;
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
        return arguments.sbpApiSampleId().<AlignerProvider>map(id -> new SbpBootstrapProvider(credentials, storage, arguments, id)).orElse(
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
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy);
            return new Aligner(getArguments(),
                    storage,
                    sampleSource,
                    sampleUpload,
                    spark,
                    new GoogleStorageJarUpload(),
                    optimizer, resultsDirectory,
                    alignmentOutputStorage);
        }
    }

    static class SbpBootstrapProvider extends AlignerProvider {

        private final int sbpSampleId;

        private SbpBootstrapProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
                final int sbpSampleId) {
            super(credentials, storage, arguments);
            this.sbpSampleId = sbpSampleId;
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ClusterOptimizer optimizer, GoogleDataproc dataproc, ResultsDirectory resultsDirectory)
                throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments());
            AmazonS3 s3 = S3.newClient(getArguments().sbpS3Url());
            SampleSource sampleSource = new SbpS3SampleSource(s3, new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3RemoteDownload(),
                    ProcessBuilder::new);
            SampleUpload sampleUpload = new CloudSampleUpload(new SbpS3FileSource(), cloudCopy);
            return new Aligner(getArguments(),
                    storage,
                     sampleSource,
                    sampleUpload,
                    dataproc,
                    new GoogleStorageJarUpload(),
                    optimizer, resultsDirectory,
                    alignmentOutputStorage);
        }
    }
}
