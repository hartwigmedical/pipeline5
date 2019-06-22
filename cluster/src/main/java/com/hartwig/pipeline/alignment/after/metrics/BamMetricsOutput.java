package com.hartwig.pipeline.alignment.after.metrics;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

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
        return maybeMetricsOutputFile().orElseThrow(() -> new IllegalStateException("No metrics file available"));
    }

    static String outputFile(String sample) {
        return String.format("%s.wgsmetrics", sample);
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
