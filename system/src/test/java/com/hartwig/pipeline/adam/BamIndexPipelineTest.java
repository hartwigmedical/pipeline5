package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.after.BamIndexPipeline;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Issues running samtools on travis")
public class BamIndexPipelineTest {

    private static final String SAMPLE_NAME = "CPCT12345678R";
    private static final String TEST_DIR = "index_test";
    private static final String SOURCE_DIR = "/source/";
    private static final String TARGET_DIR = "/target/";
    private Metric baiTimeSpent;

    @Before
    public void setUp() throws Exception {
        final Path testDir = Paths.get(Resources.targetResource(TEST_DIR));
        final BamIndexPipeline victim = BamIndexPipeline.fallback(Hadoop.localFilesystem(),
                testDir.toString() + SOURCE_DIR,
                testDir.toString() + TARGET_DIR,
                metric -> baiTimeSpent = metric);
        FileUtils.deleteDirectory(testDir.toFile());
        moveTestFilesToTarget(testDir);
        victim.execute(Sample.builder("", SAMPLE_NAME).build());
    }

    @Test
    public void sortedBamCreatedFromBam() throws Exception {
        assertThat(new File(Resources.targetResource(TEST_DIR + SOURCE_DIR + SAMPLE_NAME + ".sorted.bam"))).exists();
    }

    @Test
    public void bailFileCreatedFromBam() throws Exception {
        assertThat(new File(Resources.targetResource(TEST_DIR + SOURCE_DIR + SAMPLE_NAME + ".sorted.bam.bai"))).exists();
    }

    @Test
    public void baiCreationTimeMetricCaptured() throws Exception {
        assertThat(baiTimeSpent).isNotNull();
        assertThat(baiTimeSpent.value()).isPositive();
    }

    private void moveTestFilesToTarget(final Path resultDir) throws IOException {
        Files.createDirectory(resultDir);
        Files.createDirectory(Paths.get(resultDir.toString(), SOURCE_DIR));
        Files.copy(Paths.get(Resources.testResource("index/" + SAMPLE_NAME + ".bam")), Paths.get(resultDir.toString(), SOURCE_DIR, SAMPLE_NAME + ".bam"));
    }
}