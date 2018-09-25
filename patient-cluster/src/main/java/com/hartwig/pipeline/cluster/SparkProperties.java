package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

class SparkProperties {

    static Map<String, String> asMap(PerformanceProfile performanceProfile) {
        return ImmutableMap.<String, String>builder().put("spark.yarn.executor.memoryOverhead", performanceProfile.offHeapPerNodeGB() + "G")
                .put("spark.executor.memory", performanceProfile.heapPerNodeGB() + "G")
                .put("spark.executor.cores", String.valueOf(performanceProfile.cpuPerWorker()))
                .put("spark.executor.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.driver.extraJavaOptions", "-XX:hashCode=0").put("spark.rdd.compress", "true")
                .build();
    }
}
