package com.hartwig.pipeline.after;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class BamMetricsPipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R";
    private static final String TEST_DIR = "metric_test";
    private static final String SOURCE_DIR = "metrics/";
    private static final String OUTPUT_FILE = System.getProperty("user.dir") + "/CPCT1234567R";
    private Metric metricTimeSpent;

    @Before
    public void setUp() throws Exception {
        final Path testDir = Paths.get(Resources.targetResource(TEST_DIR));
        final BamMetricsPipeline victim = BamMetricsPipeline.create(Hadoop.localFilesystem(), Resources.testResource(SOURCE_DIR),
                Resources.testResource("reference_genome"),
                metric -> metricTimeSpent = metric);
        FileUtils.deleteDirectory(testDir.toFile());
        moveTestFilesToTarget(testDir);
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

    private void moveTestFilesToTarget(final Path resultDir) throws IOException {
        Files.createDirectory(resultDir);
        Files.createDirectory(Paths.get(resultDir.toString(), SOURCE_DIR));
        Files.copy(Paths.get(Resources.testResource(SOURCE_DIR + SAMPLE_NAME + ".sorted.bam")),
                Paths.get(resultDir.toString(), SOURCE_DIR, SAMPLE_NAME + ".sorted.bam"));
    }
}