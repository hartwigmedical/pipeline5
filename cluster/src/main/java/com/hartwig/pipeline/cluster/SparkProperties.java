package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.performance.PerformanceProfile;

class SparkProperties {

    private static final double OVERHEAD = 0.9;
    private static final double ALLOWABLE_RATIO = 0.8;
    private static final int SAFETY_GIG = 1;

    static Map<String, String> asMap(PerformanceProfile performanceProfile) {
        return ImmutableMap.<String, String>builder().put("spark.executor.memory",
                allowableExecutorMemory(performanceProfile.primaryWorkers().memoryGB()))
                .put("spark.driver.memory", allowableExecutorMemory(performanceProfile.master().memoryGB()))
                .put("spark.executor.cores", String.valueOf(performanceProfile.primaryWorkers().cpus()))
                .put("spark.executor.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.driver.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.rdd.compress", "true")
                .put("spark.memory.fraction", "0.1")
                .build();
    }

    private static String allowableExecutorMemory(final int machineMemoryGB) {
        double maxMemory = machineMemoryGB * ALLOWABLE_RATIO;
        double maxMemoryMinusOverhead = maxMemory * OVERHEAD;
        return ((int) maxMemoryMinusOverhead - SAFETY_GIG) + "G";
    }
}
