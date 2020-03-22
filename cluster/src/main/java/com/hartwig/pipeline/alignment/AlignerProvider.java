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
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSFileSource;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.LocalFileSource;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.storage.SbpS3FileSource;

public abstract class AlignerProvider {

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

    private static VmAligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
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
                Executors.newFixedThreadPool(arguments.maxConcurrentLanes()));
    }

    abstract VmAligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
            ResultsDirectory resultsDirectory) throws Exception;

    public static AlignerProvider from(GoogleCredentials credentials, Storage storage, Arguments arguments) {
        if (arguments.sbpApiRunId().isPresent() || arguments.sbpApiSampleId().isPresent()) {
            return new SbpAlignerProvider(credentials, storage, arguments);
        }
        return new LocalAlignerProvider(credentials, storage, arguments);
    }

    public VmAligner get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        AlignmentOutputStorage alignmentOutputStorage = new AlignmentOutputStorage(storage, arguments, resultsDirectory);
        return wireUp(credentials, storage, alignmentOutputStorage, resultsDirectory);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        VmAligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ResultsDirectory resultsDirectory) throws Exception {
            SampleSource sampleSource = getArguments().upload()
                    ? new FileSystemSampleSource(getArguments().sampleDirectory())
                    : new GoogleStorageSampleSource(storage, getArguments());
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new LocalFileSource(), gsUtilCloudCopy, getArguments());
            return AlignerProvider.constructVmAligner(getArguments(),
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
        VmAligner wireUp(GoogleCredentials credentials, Storage storage, AlignmentOutputStorage alignmentOutputStorage,
                ResultsDirectory resultsDirectory) throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments().sbpApiUrl());
            SampleSource sampleSource = new SbpS3SampleSource(new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3RemoteDownload(),
                    ProcessBuilder::new);
            SampleUpload sampleUpload = new CloudSampleUpload(getArguments().uploadFromGcp() ? new GSFileSource() : new SbpS3FileSource(),
                    cloudCopy,
                    getArguments());
            return AlignerProvider.constructVmAligner(getArguments(),
                    credentials,
                    storage,
                    sampleSource,
                    sampleUpload,
                    resultsDirectory,
                    alignmentOutputStorage);
        }
    }
}
