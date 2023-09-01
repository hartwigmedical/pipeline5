package com.hartwig.pipeline.metrics;

import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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

    static String intermediateOutputFile(final String sample) {
        return String.format("%s.wgsmetrics.intermediate.tmp", sample);
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
