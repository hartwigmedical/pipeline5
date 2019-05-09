package com.hartwig.pipeline.bammetrics;

import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class BamMetricsOutputStorage {

    private final Storage storage;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;

    public BamMetricsOutputStorage(final Storage storage, final Arguments arguments, final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
    }

    public BamMetricsOutput get(Sample sample) {
        RuntimeBucket metricsBucket = RuntimeBucket.from(storage, BamMetrics.NAMESPACE, sample.name(), arguments);
        return BamMetricsOutput.builder()
                .status(JobStatus.SUCCESS)
                .maybeMetricsOutputFile(GoogleStorageLocation.of(metricsBucket.name(),
                        resultsDirectory.path(BamMetricsOutput.outputFile(sample))))
                .build();
    }
}
