package com.hartwig.pipeline.after;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.junit.Before;
import org.junit.Test;

public class BamMetricsPipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R";

    private static final String PICARD_LIB_DIR =  Resources.testResource("picardlib");
    private static final String LOCAL_WORKING_DIR =  Resources.targetResource("metrics_output");
    private static final String OUTPUT_FILE = LOCAL_WORKING_DIR + File.separator + "CPCT12345678R.local.wgsmetrics";

    private Metric metricTimeSpent;

    @Before
    public void setUp() throws Exception {
        final BamMetricsPipeline victim = BamMetricsPipeline.create(Hadoop.localFilesystem(),
                Resources.testResource("metrics"),
                Resources.testResource("reference_genome"),
                LOCAL_WORKING_DIR,
                PICARD_LIB_DIR,
                metric -> metricTimeSpent = metric);
        victim.execute(Sample.builder("", SAMPLE_NAME).build());
    }

    @Test
    public void metricsFileIsCreated() {
        assertThat(new File(OUTPUT_FILE)).exists();
    }

    @Test
    public void creationTimeMetricCaptured() {
        assertThat(metricTimeSpent).isNotNull();
        assertThat(metricTimeSpent.value()).isPositive();
    }
}