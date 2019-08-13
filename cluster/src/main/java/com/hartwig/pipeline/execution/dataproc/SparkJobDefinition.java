package com.hartwig.pipeline.execution.dataproc;

import java.util.List;
import java.util.Map;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface SparkJobDefinition extends JobDefinition<DataprocPerformanceProfile> {

    String GUNZIP_MAIN = "com.hartwig.bam.runtime.GoogleCloudGunzip";
    String BAM_CREATION_MAIN = "com.hartwig.bam.runtime.GoogleCloudBamCreationRuntime";
    String SORT_INDEX_MAIN = "com.hartwig.bam.runtime.GoogleCloudSortAndIndex";

    String mainClass();

    String jarLocation();

    List<String> arguments();

    Map<String, String> sparkProperties();

    static SparkJobDefinition bamCreation(JarLocation jarLocation, Arguments arguments, RuntimeBucket runtimeBucket,
            DataprocPerformanceProfile profile) {
        String name = "BamCreation";
        return ImmutableSparkJobDefinition.builder()
                .name(name)
                .mainClass(BAM_CREATION_MAIN)
                .jarLocation(jarLocation.uri())
                .addArguments(arguments.version(), runtimeBucket.name(), arguments.project(), runtimeBucket.getNamespace(), name)
                .sparkProperties(SparkProperties.asMap(profile))
                .performanceProfile(profile)
                .build();
    }

    static SparkJobDefinition sortAndIndex(JarLocation jarLocation, Arguments arguments, RuntimeBucket runtimeBucket, Sample sample,
            ResultsDirectory resultsDirectory) {
        DataprocPerformanceProfile performanceProfile = DataprocPerformanceProfile.singleNode();
        String name = "SortAndIndex";
        return ImmutableSparkJobDefinition.builder()
                .name(name)
                .mainClass(SORT_INDEX_MAIN)
                .jarLocation(jarLocation.uri())
                .addArguments(arguments.version(),
                        runtimeBucket.name(),
                        arguments.project(),
                        sample.name(),
                        resultsPath(runtimeBucket, resultsDirectory),
                        runtimeBucket.getNamespace(), name)
                .sparkProperties(SparkProperties.asMap(performanceProfile))
                .performanceProfile(performanceProfile)
                .build();
    }

    @NotNull
    static String resultsPath(final RuntimeBucket runtimeBucket, final ResultsDirectory resultsDirectory) {
        return runtimeBucket.getNamespace() + "/" + resultsDirectory.path();
    }

    static SparkJobDefinition gunzip(JarLocation jarLocation, RuntimeBucket runtimeBucket) {
        DataprocPerformanceProfile performanceProfile = DataprocPerformanceProfile.mini();
        String name = "Gunzip";
        return ImmutableSparkJobDefinition.builder()
                .name(name)
                .mainClass(GUNZIP_MAIN)
                .jarLocation(jarLocation.uri())
                .sparkProperties(SparkProperties.asMap(performanceProfile))
                .performanceProfile(performanceProfile)
                .addArguments(runtimeBucket.getNamespace(), name)
                .build();
    }
}
