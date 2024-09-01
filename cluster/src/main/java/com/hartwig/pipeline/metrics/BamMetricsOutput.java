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

    Optional<GoogleStorageLocation> maybeMetricsOutputFile();

    default GoogleStorageLocation metricsOutputFile() {
        return maybeMetricsOutputFile().orElse(GoogleStorageLocation.empty());
    }

    static String outputFile(final String sample) {
        return String.format("%s.wgsmetrics", sample);
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
