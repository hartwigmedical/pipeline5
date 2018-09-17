package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

class SparkProperties {

    static Map<String, String> asMap() {
        return ImmutableMap.<String, String>builder().put("spark.yarn.executor.memoryOverhead", "40G")
                .put("spark.driver.memory", "64G").put("spark.executor.memory", "60G")
                .put("spark.dynamicAllocation.enabled", "false").put("spark.executor.instances", "6").put("spark.executor.cores", "25")
                .build();
    }
}
