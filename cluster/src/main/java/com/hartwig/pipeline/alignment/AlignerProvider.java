package com.hartwig.pipeline.alignment;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.bwa.BwaAligner;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpS3SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpSampleReader;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.rerun.PersistedAlignment;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSFileSource;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.RCloneCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;

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

    private static BwaAligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SampleSource sampleSource, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory) throws Exception {
        ComputeEngine computeEngine = GoogleComputeEngine.from(arguments, credentials);
        return new BwaAligner(arguments,
                computeEngine,
                storage,
                sampleSource,
                sampleUpload,
                resultsDirectory,
                Executors.newFixedThreadPool(arguments.maxConcurrentLanes()));
    }

    abstract Aligner wireUp(GoogleCredentials credentials, Storage storage, ResultsDirectory resultsDirectory) throws Exception;

    public static AlignerProvider from(GoogleCredentials credentials, Storage storage, SomaticMetadataApi api, Arguments arguments) {
        if (arguments.runFrom().isPresent() && arguments.runFrom().get().equals(Aligner.NAMESPACE)) {
            return new PersistedAlignerProvider(credentials, storage, api, arguments);
        }
        if (arguments.sbpApiRunId().isPresent()) {
            return new SbpAlignerProvider(credentials, storage, arguments);
        }
        return new LocalAlignerProvider(credentials, storage, arguments);
    }

    public Aligner get() throws Exception {
        ResultsDirectory resultsDirectory = ResultsDirectory.defaultDirectory();
        return wireUp(credentials, storage, resultsDirectory);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        BwaAligner wireUp(GoogleCredentials credentials, Storage storage, ResultsDirectory resultsDirectory) throws Exception {
            SampleSource sampleSource =
                    getArguments().sampleJson().<SampleSource>map(JsonSampleSource::new).orElse(new GoogleStorageSampleSource(storage,
                            getArguments()));
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new GSFileSource(), gsUtilCloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(), credentials, storage, sampleSource, sampleUpload, resultsDirectory);
        }
    }

    static class SbpAlignerProvider extends AlignerProvider {

        private SbpAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments) {
            super(credentials, storage, arguments);
        }

        @Override
        BwaAligner wireUp(GoogleCredentials credentials, Storage storage, ResultsDirectory resultsDirectory) throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments().sbpApiUrl());
            SampleSource sampleSource = new SbpS3SampleSource(new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new RCloneCloudCopy(getArguments().rclonePath(),
                    getArguments().rcloneGcpRemote(),
                    getArguments().rcloneS3RemoteDownload(),
                    ProcessBuilder::new);
            SampleUpload sampleUpload = new CloudSampleUpload(new GSFileSource(), cloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(), credentials, storage, sampleSource, sampleUpload, resultsDirectory);
        }
    }

    static class PersistedAlignerProvider extends AlignerProvider {

        private final SomaticMetadataApi api;

        public PersistedAlignerProvider(final GoogleCredentials credentials, final Storage storage, final SomaticMetadataApi api,
                final Arguments arguments) {
            super(credentials, storage, arguments);
            this.api = api;
        }

        @Override
        Aligner wireUp(final GoogleCredentials credentials, final Storage storage, final ResultsDirectory resultsDirectory) {
            return new PersistedAlignment(storage.get(getArguments().outputBucket()), api.get().runName(), getArguments());
        }
    }
}
