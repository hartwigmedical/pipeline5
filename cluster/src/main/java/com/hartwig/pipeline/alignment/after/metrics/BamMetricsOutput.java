package com.hartwig.pipeline.alignment.after.metrics;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface BamMetricsOutput extends StageOutput {
    JobStatus status();

    @Override
    default String name() {
        return BamMetrics.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeMetricsOutputFile();

    default GoogleStorageLocation metricsOutputFile() {
        return maybeMetricsOutputFile().orElseThrow(() -> new IllegalStateException("No metrics file available"));
    }

    static String outputFile(Sample sample) {
        return String.format("%s.wgsmetrics", sample.name());
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
