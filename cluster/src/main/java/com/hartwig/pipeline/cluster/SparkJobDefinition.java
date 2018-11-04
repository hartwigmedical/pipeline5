package com.hartwig.pipeline.cluster;

import java.util.List;
import java.util.Map;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
public interface SparkJobDefinition {

    String GUNZIP_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudGunzip";
    String BAM_CREATION_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    String SORT_INDEX_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudSortAndIndex";

    String name();

    String mainClass();

    String jarLocation();

    List<String> arguments();

    Map<String, String> sparkProperties();

    static SparkJobDefinition bamCreation(String jarLocation, Arguments arguments, RuntimeBucket runtimeBucket,
            PerformanceProfile profile) {
        return ImmutableSparkJobDefinition.builder()
                .name("BamCreation")
                .mainClass(BAM_CREATION_MAIN)
                .jarLocation(jarLocation)
                .addArguments(arguments.version(), runtimeBucket.getName(), arguments.project())
                .sparkProperties(SparkProperties.asMap(profile))
                .build();
    }

    static SparkJobDefinition sortAndIndex(String jarLocation, Arguments arguments, RuntimeBucket runtimeBucket, PerformanceProfile profile,
            Sample sample, ResultsDirectory resultsDirectory) {
        return ImmutableSparkJobDefinition.builder()
                .name("SortAndIndex")
                .mainClass(SORT_INDEX_MAIN)
                .jarLocation(jarLocation)
                .addArguments(arguments.version(), runtimeBucket.getName(), arguments.project(), sample.name(), resultsDirectory.path(""))
                .sparkProperties(SparkProperties.asMap(profile))
                .build();
    }

    static SparkJobDefinition gunzip(String jarLocation, PerformanceProfile profile) {
        return ImmutableSparkJobDefinition.builder()
                .name("Gunzip")
                .mainClass(GUNZIP_MAIN)
                .jarLocation(jarLocation)
                .sparkProperties(SparkProperties.asMap(profile))
                .build();
    }
}
