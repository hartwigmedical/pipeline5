package com.hartwig.pipeline.bammetrics;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface BamMetricsOutput {
    JobStatus status();

    GoogleStorageLocation metricsOutputFile();

    static String outputFile(Sample sample) {
        return String.format("%s.wgsmetrics", sample.name());
    }

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
