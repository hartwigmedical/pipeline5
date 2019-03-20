package com.hartwig.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.spark.SparkContexts;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;
import com.hartwig.testsupport.Lanes;
import com.hartwig.testsupport.TestConfigurations;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class GunZipTest {

    private static final JavaSparkContext SPARK_CONTEXT = SparkContexts.create("gunzip-test", TestConfigurations.HUNDREDK_READS_HISEQ);
    private static final String NOT_ZIPPED_DIRECTORY = "gunzip/not_zipped/";
    private static final String ZIPPED_DIRECTORY = "gunzip/zipped/";
    private static final String TWO_LANE_DIRECTORY = "gunzip/two_lanes_zipped/";
    private static final String NEEDS_RENAMING = "gunzip/needs_renaming/";
    private static final Lane NOT_ZIPPED_LANE = Lanes.emptyBuilder()
            .firstOfPairPath(Resources.targetResource(NOT_ZIPPED_DIRECTORY + "R1.fastq"))
            .secondOfPairPath(Resources.targetResource(NOT_ZIPPED_DIRECTORY + "R2.fastq"))
            .build();

    private static final Lane ZIPPED_LANE = Lanes.emptyBuilder()
            .firstOfPairPath(Resources.targetResource(ZIPPED_DIRECTORY + "R1.fastq.gz"))
            .secondOfPairPath(Resources.targetResource(ZIPPED_DIRECTORY + "R2.fastq.gz"))
            .build();
    private static final List<Lane> ZIPPER_TWO_LANES = Lists.newArrayList(Lanes.emptyBuilder()
                    .firstOfPairPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L1_R1.fastq.gz"))
                    .secondOfPairPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L1_R2.fastq.gz"))
                    .build(),
            Lanes.emptyBuilder()
                    .firstOfPairPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L2_R1.fastq.gz"))
                    .secondOfPairPath(Resources.targetResource(TWO_LANE_DIRECTORY + "L2_R2.fastq.gz"))
                    .build());
    private static final Lane UNZIPPED_LANE = Lanes.emptyBuilder()
            .firstOfPairPath(Resources.targetResource(ZIPPED_DIRECTORY + "R1.fastq"))
            .secondOfPairPath(Resources.targetResource(ZIPPED_DIRECTORY + "R2.fastq"))
            .build();
    private static final Lane RENAMED_LANE = Lanes.emptyBuilder()
            .firstOfPairPath(Resources.targetResource(NEEDS_RENAMING + "R1.fastq"))
            .secondOfPairPath(Resources.targetResource(NEEDS_RENAMING + "R2.fastq"))
            .build();
    private static final Lane NEEDS_RENAMING_LANE = Lanes.emptyBuilder()
            .firstOfPairPath(Resources.targetResource(NEEDS_RENAMING + "R1.fastq.gz"))
            .secondOfPairPath(Resources.targetResource(NEEDS_RENAMING + "R2.fastq.gz"))
            .build();
    private GunZip victim;
    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = Hadoop.localFilesystem();
        victim = new GunZip(fileSystem, SPARK_CONTEXT, false);
    }

    @After
    public void tearDown() throws Exception {
        deleteIfExists(Resources.targetResource("gunzip"));
    }

    @AfterClass
    public static void tearDownClass() {
        SPARK_CONTEXT.stop();
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
    public void onlyRenamesWhenFileIsAlreadyUnzipped() throws Exception {
        victim = new GunZip(fileSystem, SPARK_CONTEXT, true);
        setupSample(NEEDS_RENAMING);
        Sample notZipped = sampleBuilder().addLanes(NEEDS_RENAMING_LANE).build();
        Sample result = victim.run(notZipped);
        Lane onlyLane = result.lanes().get(0);
        checkFilesExist(onlyLane, RENAMED_LANE);
    }

    @Test
    public void zippedFilesAreDeletedAfterUnzipping() throws Exception {
        setupSample(ZIPPED_DIRECTORY);
        victim.run(sampleBuilder().addLanes(ZIPPED_LANE).build());
        assertThat(fileSystem.exists(new Path(ZIPPED_LANE.secondOfPairPath()))).isFalse();
        assertThat(fileSystem.exists(new Path(ZIPPED_LANE.firstOfPairPath()))).isFalse();
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
        assertThat(fileSystem.exists(new Path(onlyLane.firstOfPairPath()))).isTrue();
        assertThat(fileSystem.exists(new Path(onlyLane.secondOfPairPath()))).isTrue();
    }

    private static ImmutableSample.Builder sampleBuilder() {
        return Sample.builder("test", "test");
    }
}