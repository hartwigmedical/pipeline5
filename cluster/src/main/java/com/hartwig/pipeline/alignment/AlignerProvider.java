package com.hartwig.pipeline.alignment;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.bwa.BwaAligner;
import com.hartwig.pipeline.alignment.persisted.PersistedAlignment;
import com.hartwig.pipeline.alignment.sample.GoogleStorageSampleSource;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpS3SampleSource;
import com.hartwig.pipeline.alignment.sample.SbpSampleReader;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.GoogleComputeEngine;
import com.hartwig.pipeline.execution.vm.NoOpComputeEngine;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.reruns.ApiPersistedDataset;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.CloudCopy;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSFileSource;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;

public abstract class AlignerProvider {

    private final GoogleCredentials credentials;
    private final Storage storage;
    private final Arguments arguments;
    private final Labels labels;

    AlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments, final Labels labels) {
        this.credentials = credentials;
        this.storage = storage;
        this.arguments = arguments;
        this.labels = labels;
    }

    protected Arguments getArguments() {
        return arguments;
    }

    private static BwaAligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final SampleSource sampleSource, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory, final Labels labels)
            throws Exception {
        ComputeEngine computeEngine =
                arguments.publishEventsOnly() ? new NoOpComputeEngine() : GoogleComputeEngine.from(arguments, credentials, labels);
        return new BwaAligner(arguments,
                computeEngine,
                storage,
                sampleSource,
                sampleUpload,
                resultsDirectory,
                Executors.newFixedThreadPool(arguments.maxConcurrentLanes()),
                labels);
    }

    abstract Aligner wireUp(final GoogleCredentials credentials, final Storage storage, final ResultsDirectory resultsDirectory,
            final Labels labels) throws Exception;

    public static AlignerProvider from(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
            final Labels labels) {
        if (new StartingPoint(arguments).usePersisted(Aligner.NAMESPACE)) {
            return new PersistedAlignerProvider(credentials,
                    storage,
                    arguments,
                    arguments.biopsy()
                            .<PersistedDataset>map(b -> new ApiPersistedDataset(SbpRestApi.newInstance(arguments.sbpApiUrl()),
                                    ObjectMappers.get(),
                                    b,
                                    arguments.project()))
                            .orElse((new NoopPersistedDataset())),
                    labels);
        }
        if (arguments.sbpApiRunId().isPresent()) {
            return new SbpAlignerProvider(credentials, storage, arguments, labels);
        }
        return new LocalAlignerProvider(credentials, storage, arguments, labels);
    }

    public Aligner get() throws Exception {
        return wireUp(credentials, storage, ResultsDirectory.defaultDirectory(), labels);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
                final Labels labels) {
            super(credentials, storage, arguments, labels);
        }

        @Override
        BwaAligner wireUp(final GoogleCredentials credentials, final Storage storage, final ResultsDirectory resultsDirectory,
                final Labels labels) throws Exception {
            SampleSource sampleSource =
                    getArguments().sampleJson().<SampleSource>map(JsonSampleSource::new).orElse(new GoogleStorageSampleSource(storage,
                            getArguments(),
                            labels));
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new GSFileSource(), gsUtilCloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(),
                    credentials,
                    storage,
                    sampleSource,
                    sampleUpload,
                    resultsDirectory,
                    labels);
        }
    }

    static class SbpAlignerProvider extends AlignerProvider {

        private SbpAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
                final Labels labels) {
            super(credentials, storage, arguments, labels);
        }

        @Override
        BwaAligner wireUp(final GoogleCredentials credentials, final Storage storage, final ResultsDirectory resultsDirectory,
                final Labels labels) throws Exception {
            SbpRestApi sbpRestApi = SbpRestApi.newInstance(getArguments().sbpApiUrl());
            SampleSource sampleSource = new SbpS3SampleSource(new SbpSampleReader(sbpRestApi));
            CloudCopy cloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new GSFileSource(), cloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(),
                    credentials,
                    storage,
                    sampleSource,
                    sampleUpload,
                    resultsDirectory,
                    labels);
        }
    }

    static class PersistedAlignerProvider extends AlignerProvider {

        private final PersistedDataset persistedDataset;

        public PersistedAlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments,
                final PersistedDataset persistedDataset, final Labels labels) {
            super(credentials, storage, arguments, labels);
            this.persistedDataset = persistedDataset;
        }

        @Override
        Aligner wireUp(final GoogleCredentials credentials, final Storage storage, final ResultsDirectory resultsDirectory,
                final Labels labels) {
            return new PersistedAlignment(persistedDataset, getArguments(), storage);
        }
    }
}
