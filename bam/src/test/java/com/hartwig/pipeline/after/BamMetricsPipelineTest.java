package com.hartwig.pipeline.after;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Work in progress")
public class BamMetricsPipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R";
    private static final String LOCAL_WORKING_DIR =  Resources.testResource("metrics_output");
    private static final String OUTPUT_FILE = LOCAL_WORKING_DIR + File.separator + "CPCT1234567R";

    private Metric metricTimeSpent;

    @Before
    public void setUp() throws Exception {
        final BamMetricsPipeline victim = BamMetricsPipeline.create(Hadoop.localFilesystem(),
                Resources.testResource("metrics"),
                Resources.testResource("reference_genome"),
                LOCAL_WORKING_DIR,
                metric -> metricTimeSpent = metric);
        victim.execute(Sample.builder("", SAMPLE_NAME).build());
    }

    @Test
    public void metricsFileIsCreated() {
        assertThat(new File(Resources.targetResource(OUTPUT_FILE))).exists();
    }

    @Test
    public void creationTimeMetricCaptured() {
        assertThat(metricTimeSpent).isNotNull();
        assertThat(metricTimeSpent.value()).isPositive();
    }

//    private void moveTestFilesToTarget(final Path resultDir) throws IOException {
//        Files.createDirectory(resultDir);
//        Files.createDirectory(Paths.get(resultDir.toString(), SOURCE_DIR));
//        Files.copy(Paths.get(Resources.testResource(SOURCE_DIR + SAMPLE_NAME + ".sorted.bam")),
//                Paths.get(resultDir.toString(), SOURCE_DIR, SAMPLE_NAME + ".sorted.bam"));
//    }
}