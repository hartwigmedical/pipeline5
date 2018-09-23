package com.hartwig.pipeline.cluster;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

class SparkProperties {

    static Map<String, String> asMap() {
        return ImmutableMap.<String, String>builder().put("spark.yarn.executor.memoryOverhead", "48G")
                .put("spark.driver.memory", "64G")
                .put("spark.executor.memory", "38G")
                .put("spark.executor.extraJavaOptions", "-XX:hashCode=0")
                .put("spark.driver.extraJavaOptions", "-XX:hashCode=0")
                .build();
    }
}
