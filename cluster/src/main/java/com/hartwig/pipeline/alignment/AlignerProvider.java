package com.hartwig.pipeline.alignment;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.sample.FileSystemSampleSource;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpS3SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpSampleReader;
import com.hartwig.pipeline.alignment.vm.VmAligner;
import com.hartwig.pipeline.execution.dataproc.ClusterOptimizer;
import com.hartwig.pipeline.execution.dataproc.CpuFastQSizeRatio;
import com.hartwig.pipeline.execution.dataproc.GoogleDataproc;
import com.hartwig.pipeline.execution.dataproc.GoogleStorageJarUpload;
import com.hartwig.pipeline.execution.dataproc.NodeInitialization;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.LocalFileSource;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.transfer.sbp.SbpS3FileSource;
import com.hartwig.support.hadoop.Hadoop;

public abstract class AlignerProvider {

    private static final int PERFECT_RATIO = 2;
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
            ClusterOptimizer optimizer, GoogleDataproc dataproc, ResultsDirectory resultsDirectory) throws Exception;

    public Aligner get() throws Exception {
        NodeInitialization nodeInitialization = new NodeInitialization(arguments.nodeInitializationScript());
        CpuFastQSizeRatio ratio = CpuFastQSizeRatio.of(PERFECT_RATIO);
        ClusterOptimizer optimizer = new ClusterOptimizer(ratio, arguments.usePreemptibleVms());
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        GoogleDataproc dataproc = GoogleDataproc.from(credentials, nodeInitialization, arguments, resultsDirectory);
        AlignmentOutputStorage alignmentOutputStorage = new AlignmentOutputStorage(storage, arguments, resultsDirectory);
        return wireUp(credentials, storage, alignmentOutputStorage, optimizer, dataproc, resultsDirectory);
    }

    public static AlignerProvider from(GoogleCredentials credentials, Storage storage, Arguments arguments) {
        if (arguments.sbpApiRunId().isPresent() || arguments.sbpApiSampleId().isPresent()) {
            return new SbpAlignerProvider(credentials, storage, arguments);
        }
        return new LocalAlignerProvider(credentials, storage, arguments);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ClusterOptimizer optimizer, GoogleDataproc spark, ResultsDirectory resultsDirectory) throws Exception {
            SampleSource sampleSource = getArguments().upload()
                    ? new FileSystemSampleSource(Hadoop.localFilesystem(), getArguments().sampleDirectory())
                    : new GoogleStorageSampleSource(storage, getArguments());
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy);
            return getArguments().alignerType().equals(Arguments.AlignerType.SPARK)
                    ? constructDataprocAligner(getArguments(),
                    storage,
                    sampleSource,
                    sampleUpload,
                    spark,
                    new GoogleStorageJarUpload(),
                    optimizer,
                    resultsDirectory,
                    alignmentOutputStorage)
                    : AlignerProvider.constructVmAligner(getArguments(),
                            credentials,
                            storage,
                            sampleSource,
                            sampleUpload,
                            resultsDirectory,
                            alignmentOutputStorage);
        }
    }

    static class SbpAlignerProvider extends AlignerProvider {

        private SbpAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        Aligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ClusterOptimizer optimizer, GoogleDataproc dataproc, ResultsDirectory resultsDirectory) throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments());
            SampleSource sampleSource = new SbpS3SampleSource(new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3RemoteDownload(),
                    ProcessBuilder::new);
            SampleUpload sampleUpload = new CloudSampleUpload(new SbpS3FileSource(), cloudCopy);
            return getArguments().alignerType().equals(Arguments.AlignerType.SPARK)
                    ? constructDataprocAligner(getArguments(),
                    storage,
                    sampleSource,
                    sampleUpload,
                    dataproc,
                    new GoogleStorageJarUpload(),
                    optimizer,
                    resultsDirectory,
                    alignmentOutputStorage)
                    : AlignerProvider.constructVmAligner(getArguments(),
                            credentials,
                            storage,
                            sampleSource,
                            sampleUpload,
                            resultsDirectory,
                            alignmentOutputStorage);
        }
    }

    private static Aligner constructDataprocAligner(final Arguments arguments, final Storage storage, final SampleSource sampleSource,
            final SampleUpload sampleUpload, final GoogleDataproc spark, final GoogleStorageJarUpload googleStorageJarUpload,
            final ClusterOptimizer optimizer, final ResultsDirectory resultsDirectory,
            final AlignmentOutputStorage alignmentOutputStorage) {
        return new DataprocAligner(arguments,
                storage,
                sampleSource,
                sampleUpload,
                spark,
                googleStorageJarUpload,
                optimizer,
                resultsDirectory,
                alignmentOutputStorage);
    }

    private static Aligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SampleSource sampleSource, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory,
            final AlignmentOutputStorage alignmentOutputStorage) throws Exception {
        ComputeEngine computeEngine = ComputeEngine.from(arguments, credentials);
        return new VmAligner(arguments,
                computeEngine,
                storage,
                sampleSource,
                sampleUpload,
                resultsDirectory,
                alignmentOutputStorage,
                Executors.newCachedThreadPool());
    }
}
