package com.hartwig.pipeline.calling.germline;

import java.time.Duration;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class GermlineCallerOutputStorage {

    private static final int FOUR_HOURS_IN_SECONDS = 60 * 240;
    private final Storage storage;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;
    private final int timeoutInSeconds;
    private final int retryInSeconds;

    public GermlineCallerOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory) {
        this(storage, arguments, resultsDirectory, FOUR_HOURS_IN_SECONDS, 5);
    }

    private GermlineCallerOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory,
            final int timeoutInSeconds, final int retryInSeconds) {
        this.storage = storage;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.timeoutInSeconds = timeoutInSeconds;
        this.retryInSeconds = retryInSeconds;
    }

    public GermlineCallerOutput get(SingleSampleRunMetadata sample) {
        final RuntimeBucket germlineBucket = RuntimeBucket.from(storage, GermlineCaller.NAMESPACE, sample, arguments);
        final String finalVcfFile = sample + ".gonlv5.annotated." + OutputFile.GZIPPED_VCF;
        Blob metricsBlob = Failsafe.with(new RetryPolicy<>().handleResult(null)
                .withDelay(Duration.ofSeconds(retryInSeconds))
                .withMaxDuration(Duration.ofSeconds(timeoutInSeconds))).get(() -> germlineBucket.get(resultsDirectory.path(finalVcfFile)));
        if (metricsBlob != null) {
            return GermlineCallerOutput.builder()
                    .status(PipelineStatus.SUCCESS)
                    .maybeGermlineVcfLocation(GoogleStorageLocation.of(germlineBucket.name(), resultsDirectory.path(finalVcfFile)))
                    .build();
        }
        throw new IllegalStateException(String.format(
                "No germline present in [%s], waited [%s] seconds. Check that single sample pipelines have been run, have not failed, and "
                        + "are not still running metrics",
                germlineBucket.name(),
                timeoutInSeconds));
    }
}
