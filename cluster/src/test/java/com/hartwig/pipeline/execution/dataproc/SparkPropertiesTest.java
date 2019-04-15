package com.hartwig.pipeline.execution.dataproc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class SparkPropertiesTest {

    private static final DataprocPerformanceProfile PERFORMANCE_PROFILE =
            DataprocPerformanceProfile.builder().numPreemtibleWorkers(1).numPrimaryWorkers(1).build();
    private Map<String, String> victim;

    @Before
    public void setUp() throws Exception {
        victim = SparkProperties.asMap(PERFORMANCE_PROFILE);
    }

    @Test
    public void memoryAdjustedForMaxAllowableAndOverhead() {
        assertThat(victim.get("spark.executor.memory")).isEqualTo("148G");
    }
}