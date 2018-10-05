package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.performance.PerformanceProfile;

class SparkProperties {

    private static final double OVERHEAD = 0.9;
    private static final double ALLOWABLE_RATIO = 0.8;
    private static final double CPU_RATIO = 0.65;
    private static final int SAFETY_GIG = 1;

    static Map<String, String> asMap(PerformanceProfile performanceProfile) {
        return ImmutableMap.<String, String>builder().put("spark.executor.memory", allowableExecutorMemory(performanceProfile) + "G")
                .put("spark.executor.cores", String.valueOf((int) (performanceProfile.primaryWorkers().cpus() * CPU_RATIO)))
                .put("spark.executor.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.driver.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.rdd.compress", "true")
                .put("spark.memory.fraction", "0.3")
                .build();
    }

    private static int allowableExecutorMemory(final PerformanceProfile performanceProfile) {
        double maxMemory = performanceProfile.primaryWorkers().memoryGB() * ALLOWABLE_RATIO;
        double maxMemoryMinusOverhead = maxMemory * OVERHEAD;
        return (int) maxMemoryMinusOverhead - SAFETY_GIG;
    }
}
