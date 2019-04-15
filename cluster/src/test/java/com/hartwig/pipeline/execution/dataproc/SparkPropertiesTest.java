package com.hartwig.pipeline.execution.dataproc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import com.hartwig.pipeline.execution.dataproc.SparkProperties;
import com.hartwig.pipeline.performance.ImmutablePerformanceProfile;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.junit.Before;
import org.junit.Test;

public class SparkPropertiesTest {

    private static final ImmutablePerformanceProfile PERFORMANCE_PROFILE =
            PerformanceProfile.builder().numPreemtibleWorkers(1).numPrimaryWorkers(1).build();
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