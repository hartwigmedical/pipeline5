package com.hartwig.pipeline.metrics;

import java.time.Duration;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class BamMetricsOutputStorage {

    private static final int FOUR_HOURS_IN_SECONDS = 60 * 240;
    private final Storage storage;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;
    private final int timeoutInSeconds;
    private final int retryInSeconds;

    public BamMetricsOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory) {
        this(storage, arguments, resultsDirectory, FOUR_HOURS_IN_SECONDS, 5);
    }

    BamMetricsOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory,
            final int timeoutInSeconds, final int retryInSeconds) {
        this.storage = storage;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.timeoutInSeconds = timeoutInSeconds;
        this.retryInSeconds = retryInSeconds;
    }

    public BamMetricsOutput get(SingleSampleRunMetadata sample) {
        RuntimeBucket metricsBucket = RuntimeBucket.from(storage, BamMetrics.NAMESPACE, sample, arguments);
        final String metricsFile = BamMetricsOutput.outputFile(sample.sampleName());
        Blob metricsBlob = Failsafe.with(new RetryPolicy<>().handleResult(null)
                .withDelay(Duration.ofSeconds(retryInSeconds))
                .withMaxDuration(Duration.ofSeconds(timeoutInSeconds))).get(() -> metricsBucket.get(resultsDirectory.path(metricsFile)));
        if (metricsBlob != null) {
            return BamMetricsOutput.builder()
                    .status(PipelineStatus.SUCCESS)
                    .sample(sample.sampleName())
                    .maybeMetricsOutputFile(GoogleStorageLocation.of(metricsBucket.name(), resultsDirectory.path(metricsFile)))
                    .build();
        }
        throw new IllegalStateException(String.format(
                "No metrics present in [%s], waited [%s] seconds. Check that single sample pipelines have been run, have not failed, and "
                        + "are not still running metrics",
                metricsBucket.name(),
                timeoutInSeconds));
    }
}
