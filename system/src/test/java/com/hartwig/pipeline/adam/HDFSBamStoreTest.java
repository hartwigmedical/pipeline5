package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.FinalDataLocation;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.testsupport.TestRDDs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.junit.Test;

public class HDFSBamStoreTest {

    private static final Sample SAMPLE = Sample.builder("", "test").build();

    @Test
    public void savesBamAndChecksIfItExists() throws Exception {
        FileSystem fileSystem = Hadoop.localFilesystem();
        FinalDataLocation dataLocation = new FinalDataLocation(fileSystem, System.getProperty("user.dir") + "/results/");
        OutputStore<AlignmentRecordDataset> victim = new HDFSBamStore(dataLocation, fileSystem, true);
        victim.store(InputOutput.of(SAMPLE, TestRDDs.alignmentRecordDataset("qc/CPCT12345678R.bam")));
        assertThat(victim.exists(SAMPLE)).isTrue();
        fileSystem.delete(new Path(dataLocation.uri(SAMPLE, "")), true);
    }
}