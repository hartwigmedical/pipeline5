package com.hartwig.pipeline.after;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

    private static final String PICARD_JAR_PATH = Resources.testResource("picard/picard.jar");
    private static final String LOCAL_WORKING_DIR = Resources.targetResource("metrics_output");
    private static final String OUTPUT_FILE = LOCAL_WORKING_DIR + File.separator + "CPCT12345678R.wgsmetrics";

    private Metric metricTimeSpent;

    @Before
    public void setUp() throws IOException, InterruptedException {
        final BamMetricsPipeline victim = BamMetricsPipeline.create(PICARD_JAR_PATH,
                Hadoop.localFilesystem(),
                LOCAL_WORKING_DIR,
                Resources.testResource("reference_genome"),
                LOCAL_WORKING_DIR,
                metric -> metricTimeSpent = metric);
        FileUtils.deleteDirectory(new File(LOCAL_WORKING_DIR));
        moveTestBamToLocalWorkingDir(LOCAL_WORKING_DIR);
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

    private void moveTestBamToLocalWorkingDir(final String resultDir) throws IOException {
        Files.createDirectory(new File(resultDir).toPath());
        Files.copy(Paths.get(Resources.testResource("metrics/" + SAMPLE_NAME + ".sorted.bam")),
                Paths.get(resultDir, SAMPLE_NAME + ".sorted.bam"));
    }
}