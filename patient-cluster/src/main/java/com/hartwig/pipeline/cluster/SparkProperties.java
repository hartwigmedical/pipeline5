package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

class SparkProperties {

    static Map<String, String> asMap() {
        return ImmutableMap.<String, String>builder().put("spark.yarn.executor.memoryOverhead", "10G")
                .put("spark.driver.memory", "64G")
                .put("spark.executor.memory", "40G")
                .put("spark.dynamicAllocation.enabled", "false")
                .put("spark.executor.instances", "8")
                .put("spark.executor.cores", "15")
                .build();
    }
}
