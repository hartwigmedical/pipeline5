package com.hartwig.pipeline.execution.dataproc;

import java.util.List;
import java.util.Map;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.immutables.value.Value;

@Value.Immutable
public interface SparkJobDefinition extends JobDefinition<DataprocPerformanceProfile>{

    String GUNZIP_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudGunzip";
    String BAM_CREATION_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    String SORT_INDEX_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudSortAndIndex";
    String BAM_METRICS_MAIN = "com.hartwig.pipeline.runtime.GoogleCloudBamMetrics";

    String mainClass();

    String jarLocation();

    List<String> arguments();

    Map<String, String> sparkProperties();

    static SparkJobDefinition bamCreation(JarLocation jarLocation, Arguments arguments, RuntimeBucket runtimeBucket,
            DataprocPerformanceProfile profile) {
        return ImmutableSparkJobDefinition.builder()
                .name("BamCreation")
                .mainClass(BAM_CREATION_MAIN)
                .jarLocation(jarLocation.uri())
                .addArguments(arguments.version(), runtimeBucket.name(), arguments.project())
                .sparkProperties(SparkProperties.asMap(profile))
                .performanceProfile(profile)
                .build();
    }

    static SparkJobDefinition sortAndIndex(JarLocation jarLocation, Arguments arguments, RuntimeBucket runtimeBucket, Sample sample,
            ResultsDirectory resultsDirectory) {
        DataprocPerformanceProfile performanceProfile = DataprocPerformanceProfile.mini();
        return ImmutableSparkJobDefinition.builder()
                .name("SortAndIndex")
                .mainClass(SORT_INDEX_MAIN)
                .jarLocation(jarLocation.uri())
                .addArguments(arguments.version(), runtimeBucket.name(), arguments.project(), sample.name(), resultsDirectory.path(""))
                .sparkProperties(SparkProperties.asMap(performanceProfile))
                .performanceProfile(performanceProfile)
                .build();
    }

    static SparkJobDefinition bamMetrics(JarLocation jarLocation, Arguments arguments, RuntimeBucket runtimeBucket,
            DataprocPerformanceProfile profile, Sample sample) {
        return ImmutableSparkJobDefinition.builder()
                .name("BamMetrics")
                .mainClass(BAM_METRICS_MAIN)
                .jarLocation(jarLocation.uri())
                .addArguments(arguments.version(), runtimeBucket.name(), sample.name())
                .sparkProperties(SparkProperties.asMap(profile))
                .performanceProfile(profile)
                .build();
    }

    static SparkJobDefinition gunzip(JarLocation jarLocation) {
        DataprocPerformanceProfile performanceProfile = DataprocPerformanceProfile.mini();
        return ImmutableSparkJobDefinition.builder()
                .name("Gunzip")
                .mainClass(GUNZIP_MAIN)
                .jarLocation(jarLocation.uri())
                .sparkProperties(SparkProperties.asMap(performanceProfile))
                .performanceProfile(performanceProfile)
                .build();
    }

    static SparkJobDefinition tool(JarLocation jarLocation, String mainClass) {
        DataprocPerformanceProfile performanceProfile = DataprocPerformanceProfile.mini();
        return ImmutableSparkJobDefinition.builder()
                .name("Tool")
                .mainClass(mainClass)
                .jarLocation(jarLocation.uri())
                .sparkProperties(SparkProperties.asMap(performanceProfile))
                .performanceProfile(performanceProfile)
                .build();
    }
}
