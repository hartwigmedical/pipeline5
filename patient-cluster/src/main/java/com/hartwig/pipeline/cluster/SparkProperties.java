package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

class SparkProperties {

    static Map<String, String> asMap() {
        return ImmutableMap.<String, String>builder().put("spark.dynamicAllocation.enabled", "false")
                .put("spark.executor.instances", "5")
                .put("spark.executor.cores", "16")
                .put("spark.yarn.executor.memoryOverhead", "64G")
                .put("spark.driver.memory", "64G")
                .build();
    }
}
