package com.hartwig.bam.adam;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import com.hartwig.bam.runtime.spark.SparkContexts;
import com.hartwig.io.FinalDataLocation;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.testsupport.TestConfigurations;
import com.hartwig.testsupport.TestRDDs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class HDFSBamStoreTest {

    private static final JavaSparkContext SPARK_CONTEXT =
            SparkContexts.create("hdfs-bam-store-test", TestConfigurations.HUNDREDK_READS_HISEQ);
    private static final Sample SAMPLE = Sample.builder("", "test").build();
    private static final String RESULTS_PATH = System.getProperty("user.dir") + "/results/";
    private FinalDataLocation dataLocation;
    private FileSystem fileSystem;
    private OutputStore<AlignmentRecordDataset> victim;

    @Before
    public void setUp() throws Exception {
        fileSystem = Hadoop.localFilesystem();
        dataLocation = new FinalDataLocation(fileSystem, RESULTS_PATH);
        victim = new HDFSBamStore(dataLocation, fileSystem, true);
    }

    @AfterClass
    public static void afterClass() {
        SPARK_CONTEXT.stop();
    }

    @Test
    public void savesBamAndChecksIfItExists() throws Exception {
        victim.store(InputOutput.of(SAMPLE, TestRDDs.alignmentRecordDataset("qc/CPCT12345678R.bam", SPARK_CONTEXT)));
        assertThat(victim.exists(SAMPLE)).isTrue();
        fileSystem.delete(new Path(dataLocation.uri(SAMPLE, "")), true);
    }

    @Test
    public void clearsEverythingInStoreExceptStatusFiles() throws Exception {
        deleteAll();
        fileSystem.create(new Path(RESULTS_PATH + "/file1.txt"));
        fileSystem.create(new Path(RESULTS_PATH + "/Gunzip_SUCCESS"));
        fileSystem.create(new Path(RESULTS_PATH + "/BamCreation_FAILURE"));
        victim.clear();

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(RESULTS_PATH));
        assertThat(fileStatuses).hasSize(2);
        deleteAll();
    }

    private void deleteAll() throws IOException {
        for (FileStatus fileStatus : fileSystem.listStatus(new Path(RESULTS_PATH))) {
            fileSystem.delete(fileStatus.getPath(), true);
        }
    }
}