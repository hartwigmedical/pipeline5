package com.hartwig.pipeline.output;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.input.RunMetadata;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineOutputComposerProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOutputComposerProvider.class);
    private final Storage storage;
    private final Arguments arguments;
    private final String version;

    private PipelineOutputComposerProvider(final Storage storage, final Arguments arguments, final String version) {
        this.storage = storage;
        this.arguments = arguments;
        this.version = version;
    }

    public static PipelineOutputComposerProvider from(final Storage storage, final Arguments arguments, final String version) {
        return new PipelineOutputComposerProvider(storage, arguments, version);
    }

    public PipelineOutputComposer get() {
        return !arguments.publishEventsOnly() ? new ComposeInPipelineOutputBucket(version, storage, createReportBucket()) : noopComposer();
    }

    @Nullable
    private Bucket createReportBucket() {
        Bucket reportBucket = storage.get(arguments.outputBucket());
        if (reportBucket == null) {
            if (!arguments.publishEventsOnly()) {
                BucketInfo.Builder builder = BucketInfo.newBuilder(arguments.outputBucket())
                        .setStorageClass(StorageClass.STANDARD)
                        .setAutoclass(BucketInfo.Autoclass.newBuilder().setEnabled(true).build())
                        .setLocation(arguments.region());
                arguments.cmek().ifPresent(builder::setDefaultKmsKeyName);
                reportBucket = storage.create(builder.build());
            } else {
                LOGGER.warn("Output bucket [{}] does not exist and pipeline invoked in publish-only mode", arguments.outputBucket());
            }
        }
        return reportBucket;
    }

    @NotNull
    private static PipelineOutputComposer noopComposer() {
        return new PipelineOutputComposer() {
            @Override
            public <T extends StageOutput> T add(final T stageOutput) {
                return stageOutput;
            }

            @Override
            public void compose(final RunMetadata metadata, final Folder root) {
                LOGGER.info("Skipping composition as this pipeline invocation will only publish events");
            }
        };
    }
}
