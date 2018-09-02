package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;
import com.hartwig.testsupport.Lanes;
import com.hartwig.testsupport.SparkContextSingleton;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GunZipTest {

    private static final JavaSparkContext SPARK_CONTEXT = SparkContextSingleton.instance();
    private static final String NOT_ZIPPED_DIRECTORY = "gunzip/not_zipped/";
    private static final String ZIPPED_DIRECTORY = "gunzip/zipped/";
    private static final String TWO_LANE_DIRECTORY = "gunzip/two_lanes_zipped/";
    private static final Lane NOT_ZIPPED_LANE = Lanes.emptyBuilder()
            .readsPath(Resources.targetResource(NOT_ZIPPED_DIRECTORY + "R1.fastq"))
            .matesPath(Resources.targetResource(NOT_ZIPPED_DIRECTORY + "R2.fastq"))
            .build();

    private static final Lane ZIPPED_LANE = Lanes.emptyBuilder()
            .readsPath(Resources.targetResource(ZIPPED_DIRECTORY + "R1.fastq.gz"))
            .matesPath(Resources.targetResource(ZIPPED_DIRECTORY + "R2.fastq.gz"))
            .build();
    private static final List<Lane> ZIPPER_TWO_LANES = Lists.newArrayList(Lanes.emptyBuilder()
                    .readsPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L1_R1.fastq.gz"))
                    .matesPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L1_R2.fastq.gz"))
                    .build(),
            Lanes.emptyBuilder()
                    .readsPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L2_R1.fastq.gz"))
                    .matesPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L2_R2.fastq.gz"))
                    .build());
    private static final Lane UNZIPPED_LANE = Lanes.emptyBuilder()
            .readsPath(Resources.targetResource(ZIPPED_DIRECTORY + "R1.fastq"))
            .matesPath(Resources.targetResource(ZIPPED_DIRECTORY + "R2.fastq"))
            .build();
    private GunZip victim;
    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = Hadoop.localFilesystem();
        victim = new GunZip(fileSystem, SPARK_CONTEXT);
    }

    @After
    public void tearDown() throws Exception {
        deleteIfExists(Resources.targetResource("gunzip"));
    }

    private void deleteIfExists(final String filePathName) throws IOException {
        Path path = new Path(filePathName);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    @Test
    public void sampleIsEmptyThenNoUnzipping() throws Exception {
        Sample empty = sampleBuilder().build();
        Sample result = victim.run(empty);
        assertThat(result.lanes()).isEmpty();
    }

    @Test
    public void sampleNotZippedThenNoUnzipping() throws Exception {
        setupSample(NOT_ZIPPED_DIRECTORY);
        Sample notZipped = sampleBuilder().addLanes(NOT_ZIPPED_LANE).build();
        Sample result = victim.run(notZipped);
        Lane onlyLane = result.lanes().get(0);
        checkFilesExist(onlyLane, NOT_ZIPPED_LANE);
    }

    @Test
    public void zippedLanesAreUnzipped() throws Exception {
        setupSample(ZIPPED_DIRECTORY);
        Sample notZipped = sampleBuilder().addLanes(ZIPPED_LANE).build();
        Sample result = victim.run(notZipped);
        Lane onlyLane = result.lanes().get(0);
        checkFilesExist(onlyLane, UNZIPPED_LANE);
    }

    @Test
    public void zippedFilesAreDeletedAfterUnzipping() throws Exception {
        setupSample(ZIPPED_DIRECTORY);
        victim.run(sampleBuilder().addLanes(ZIPPED_LANE).build());
        assertThat(fileSystem.exists(new Path(ZIPPED_LANE.matesPath()))).isFalse();
        assertThat(fileSystem.exists(new Path(ZIPPED_LANE.readsPath()))).isFalse();
    }

    @Test
    public void supportsMultipleLanes() throws Exception {
        setupSample(TWO_LANE_DIRECTORY);
        victim.run(sampleBuilder().lanes(ZIPPER_TWO_LANES).build());
        assertThat(fileSystem.listStatus(new Path(Resources.targetResource(TWO_LANE_DIRECTORY)))).hasSize(4);
    }

    private void setupSample(String directory) throws IOException {
        fileSystem.copyFromLocalFile(new Path(Resources.testResource(directory)), new Path(Resources.targetResource(directory)));
    }

    private void checkFilesExist(final Lane onlyLane, final Lane notZippedLane) throws IOException {
        assertThat(onlyLane).isEqualTo(notZippedLane);
        assertThat(fileSystem.exists(new Path(onlyLane.readsPath()))).isTrue();
        assertThat(fileSystem.exists(new Path(onlyLane.matesPath()))).isTrue();
    }

    private static ImmutableSample.Builder sampleBuilder() {
        return Sample.builder("test", "test");
    }
}