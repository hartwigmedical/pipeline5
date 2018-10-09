package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.IndexBam;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class IndexBamTest {

    private static final String SAMPLE_NAME = "CPCT12345678R";
    private Metric baiTimeSpent;

    @Before
    public void setUp() throws Exception {
        final Path resultDir = Paths.get(Resources.targetResource("index_test"));
        final IndexBam victim = new IndexBam(Hadoop.localFilesystem(), resultDir.toString(), metric -> baiTimeSpent = metric);
        FileUtils.deleteDirectory(resultDir.toFile());
        moveTestFilesToTarget(resultDir);
        victim.execute(Sample.builder("", SAMPLE_NAME).build());
    }

    @Test
    public void baiCreatedAlongsideBam() throws Exception {
        assertThat(new File(Resources.targetResource("index_test/" + SAMPLE_NAME + ".bam.bai"))).exists();
    }

    @Test
    public void baiCreationTimeMetricCaptured() throws Exception {
        assertThat(baiTimeSpent).isNotNull();
        assertThat(baiTimeSpent.value()).isPositive();
    }

    private void moveTestFilesToTarget(final Path resultDir) throws IOException {
        Files.createDirectory(resultDir);
        Files.copy(Paths.get(Resources.testResource("index/" + SAMPLE_NAME + ".bam")),
                Paths.get(resultDir.toString() + "/" + SAMPLE_NAME + ".bam"));
    }
}