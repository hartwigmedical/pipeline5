package com.hartwig.pipeline.alignment;

import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.vm.ComputeEngine;
import com.hartwig.computeengine.execution.vm.GoogleComputeEngine;
import com.hartwig.computeengine.execution.vm.NoOpComputeEngine;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.ArgumentUtil;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.bwa.BwaAligner;
import com.hartwig.pipeline.alignment.persisted.PersistedAlignment;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.reruns.InputPersistedDataset;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.StartingPoint;
import com.hartwig.pipeline.storage.CloudSampleUpload;
import com.hartwig.pipeline.storage.GSFileSource;
import com.hartwig.pipeline.storage.GSUtilCloudCopy;
import com.hartwig.pipeline.storage.SampleUpload;

public abstract class AlignerProvider {

    private final GoogleCredentials credentials;
    private final Storage storage;
    private final Arguments arguments;
    private final PipelineInput input;
    private final Labels labels;

    AlignerProvider(final GoogleCredentials credentials, final Storage storage, final Arguments arguments, final PipelineInput input,
            final Labels labels) {
        this.credentials = credentials;
        this.storage = storage;
        this.arguments = arguments;
        this.input = input;
        this.labels = labels;
    }

    protected Arguments getArguments() {
        return arguments;
    }

    private static BwaAligner constructVmAligner(final Arguments arguments, final GoogleCredentials credentials, final Storage storage,
            final PipelineInput input, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory, final Labels labels)
            throws Exception {
        var computeEngineConfig = ArgumentUtil.toComputeEngineConfig(arguments);
        ComputeEngine computeEngine = arguments.publishEventsOnly()
                ? new NoOpComputeEngine()
                : GoogleComputeEngine.from(computeEngineConfig, credentials, labels.asMap());
        return new BwaAligner(arguments,
                computeEngine,
                storage,
                input,
                sampleUpload,
                resultsDirectory,
                Executors.newFixedThreadPool(arguments.maxConcurrentLanes()),
                labels);
    }

    abstract Aligner wireUp(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
            final ResultsDirectory resultsDirectory, final Labels labels) throws Exception;

    public static AlignerProvider from(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
            final Arguments arguments, final Labels labels) {
        if (new StartingPoint(arguments).usePersisted(Aligner.NAMESPACE)) {
            return new PersistedAlignerProvider(input,
                    credentials,
                    storage,
                    arguments,
                    new InputPersistedDataset(input, arguments.project()),
                    labels);
        }
        return new LocalAlignerProvider(input, credentials, storage, arguments, labels);
    }

    public Aligner get() throws Exception {
        return wireUp(input, credentials, storage, ResultsDirectory.defaultDirectory(), labels);
    }

    static class LocalAlignerProvider extends AlignerProvider {

        private LocalAlignerProvider(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
                final Arguments arguments, final Labels labels) {
            super(credentials, storage, arguments, input, labels);
        }

        @Override
        BwaAligner wireUp(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
                final ResultsDirectory resultsDirectory, final Labels labels) throws Exception {
            GSUtilCloudCopy gsUtilCloudCopy = new GSUtilCloudCopy(getArguments().cloudSdkPath());
            SampleUpload sampleUpload = new CloudSampleUpload(new GSFileSource(), gsUtilCloudCopy);
            return AlignerProvider.constructVmAligner(getArguments(), credentials, storage, input, sampleUpload, resultsDirectory, labels);
        }
    }

    static class PersistedAlignerProvider extends AlignerProvider {

        private final PersistedDataset persistedDataset;

        public PersistedAlignerProvider(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
                final Arguments arguments, final PersistedDataset persistedDataset, final Labels labels) {
            super(credentials, storage, arguments, input, labels);
            this.persistedDataset = persistedDataset;
        }

        @Override
        Aligner wireUp(final PipelineInput input, final GoogleCredentials credentials, final Storage storage,
                final ResultsDirectory resultsDirectory, final Labels labels) {
            return new PersistedAlignment(persistedDataset, getArguments(), storage);
        }
    }
}
