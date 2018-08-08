package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.DataLocation;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.hadoop.Hadoop;
import com.hartwig.testsupport.TestConfigurations;
import com.hartwig.testsupport.TestRDDs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.junit.Test;

public class HDFSBamStoreTest {

    private static final Sample SAMPLE = Sample.builder("", "test").build();

    @Test
    public void savesBamAndChecksIfItExists() throws Exception {
        FileSystem fileSystem = Hadoop.fileSystem(TestConfigurations.HUNDREDK_READS_HISEQ);
        DataLocation dataLocation = new DataLocation(fileSystem, System.getProperty("user.dir") + "/results/");
        OutputStore<AlignmentRecordRDD> victim = new HDFSBamStore(dataLocation, fileSystem, true);
        victim.store(InputOutput.of(OutputType.ALIGNED, SAMPLE, TestRDDs.alignmentRecordRDD("qc/CPCT12345678R_duplicate_marked.bam")));
        assertThat(victim.exists(SAMPLE, OutputType.ALIGNED)).isTrue();
        fileSystem.delete(new Path(dataLocation.uri(OutputType.ALIGNED, SAMPLE)), true);
    }
}