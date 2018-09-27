package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import com.hartwig.pipeline.performance.PerformanceProfile;

import org.junit.Test;

public class SparkPropertiesTest {

    @Test
    public void memoryAdjustedForMaxAllowableAndOverhead() {
        Map<String, String> victim =
                SparkProperties.asMap(PerformanceProfile.builder().numPreemtibleWorkers(1).numPrimaryWorkers(1).build());
        assertThat(victim.get("spark.executor.memory")).isEqualTo("85G");
    }
}