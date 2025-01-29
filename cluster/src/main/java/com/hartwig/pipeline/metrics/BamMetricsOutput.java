package com.hartwig.pipeline.metrics;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface BamMetricsOutput extends StageOutput {

    String sample();

    PipelineStatus status();

    @Override
    default String name() {
        return BamMetrics.NAMESPACE;
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }

    Optional<BamMetricsOutputLocations> maybeOutputLocations();

    default BamMetricsOutputLocations outputLocations()
    {
        return maybeOutputLocations().orElse(BamMetricsOutputLocations.builder()
                .summary(GoogleStorageLocation.empty())
                .flagCounts(GoogleStorageLocation.empty())
                .fragmentLengths(GoogleStorageLocation.empty())
                .partitionStats(GoogleStorageLocation.empty())
                .coverage(GoogleStorageLocation.empty())
                .outputDirectory(GoogleStorageLocation.empty())
                .build());
    }
}
