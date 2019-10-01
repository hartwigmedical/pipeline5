package com.hartwig.pipeline;

import java.time.Duration;
import java.util.function.Function;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class OutputStorage<S extends StageOutput, M extends RunMetadata> {

    private final ResultsDirectory resultsDirectory;

    private final Function<M, RuntimeBucket> runtimeBucketProvider;

    OutputStorage(final ResultsDirectory resultsDirectory, final Function<M, RuntimeBucket> runtimeBucketProvider) {
        this.resultsDirectory = resultsDirectory;
        this.runtimeBucketProvider = runtimeBucketProvider;
    }

    public S get(M metadata, final Stage<S, M> stage) {
        final RuntimeBucket runtimeBucket = runtimeBucketProvider.apply(metadata);
        Blob metricsBlob =
                Failsafe.with(new RetryPolicy<>().handleResult(null).withDelay(Duration.ofSeconds(5)).withMaxRetries(-1))
                        .get(() -> runtimeBucket.get(BashStartupScript.JOB_SUCCEEDED_FLAG));
        if (metricsBlob != null) {
            return stage.output(metadata, PipelineStatus.SUCCESS, runtimeBucket, resultsDirectory);
        }
        throw new IllegalStateException(String.format("No output found for stage [%s]", stage.namespace()));
    }
}
