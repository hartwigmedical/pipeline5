package com.hartwig.pipeline.bootstrap;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.GoogleStorageStatusCheck;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Stage;
import com.hartwig.pipeline.performance.PerformanceProfile;

class Jobs {

    static Job gunzip(final SparkCluster cluster, final CostCalculator costCalculator, final Monitor monitor, final JarLocation location) {
        return new Job(PerformanceProfile.singleNode(),
                cluster,
                SparkJobDefinition.gunzip(location, PerformanceProfile.singleNode()),
                Stage.gunzip(PerformanceProfile.mini()),
                costCalculator,
                monitor,
                StatusCheck.alwaysSuccess());
    }

    static Job bam(final SparkCluster cluster, final CostCalculator costCalculator, final Monitor monitor, final JarLocation location,
            final PerformanceProfile profile, final RuntimeBucket runtimeBucket, final Arguments arguments) {
        return new Job(profile,
                cluster,
                SparkJobDefinition.bamCreation(location, arguments, runtimeBucket, profile),
                Stage.bam(profile),
                costCalculator,
                monitor,
                new GoogleStorageStatusCheck(ResultsDirectory.defaultDirectory()));
    }

    static Job sortAndIndex(final SparkCluster cluster, final CostCalculator costCalculator, final Monitor monitor,
            final JarLocation location, final RuntimeBucket runtimeBucket, final Arguments arguments, final Sample sample) {
        return new Job(PerformanceProfile.singleNode(),
                cluster,
                SparkJobDefinition.sortAndIndex(location,
                        arguments,
                        runtimeBucket,
                        PerformanceProfile.singleNode(),
                        sample,
                        ResultsDirectory.defaultDirectory()),
                Stage.sortAndIndex(PerformanceProfile.singleNode()),
                costCalculator,
                monitor,
                StatusCheck.alwaysSuccess());
    }

    static Job bamMetrics(final SparkCluster cluster, final CostCalculator costCalculator, final Monitor monitor,
            final JarLocation location, final RuntimeBucket runtimeBucket, final Arguments arguments, final Sample sample) {
        return new Job(PerformanceProfile.singleNode(),
                cluster,
                SparkJobDefinition.bamMetrics(location,
                        arguments,
                        runtimeBucket,
                        PerformanceProfile.singleNode(),
                        sample,
                        ResultsDirectory.defaultDirectory()),
                Stage.bamMetrics(PerformanceProfile.singleNode()),
                costCalculator,
                monitor,
                StatusCheck.alwaysSuccess());
    }
}
