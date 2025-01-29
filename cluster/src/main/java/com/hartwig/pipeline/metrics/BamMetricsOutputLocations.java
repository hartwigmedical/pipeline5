package com.hartwig.pipeline.metrics;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.linx.ImmutableLinxSomaticOutputLocations;

import org.immutables.value.Value;

@Value.Immutable
public interface BamMetricsOutputLocations
{
    GoogleStorageLocation summary();
    GoogleStorageLocation coverage();
    GoogleStorageLocation fragmentLengths();
    GoogleStorageLocation flagCounts();
    GoogleStorageLocation partitionStats();

    GoogleStorageLocation outputDirectory();

    static ImmutableBamMetricsOutputLocations.Builder builder() {
        return ImmutableBamMetricsOutputLocations.builder();
    }
}
