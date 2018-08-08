package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataLocation;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class HDFSBamStore implements OutputStore<AlignmentRecordRDD> {

    private final DataLocation dataLocation;
    private final FileSystem fileSystem;
    private final boolean saveAsFile;

    HDFSBamStore(final DataLocation dataLocation, final FileSystem fileSystem, final boolean saveAsFile) {
        this.dataLocation = dataLocation;
        this.fileSystem = fileSystem;
        this.saveAsFile = saveAsFile;
    }

    @Override
    public void store(final InputOutput<AlignmentRecordRDD> inputOutput) {
        JavaSaveArgs saveArgs = new JavaSaveArgs(dataLocation.uri(inputOutput.type(), inputOutput.sample()),
                128 * 1024 * 1024,
                1024 * 1024,
                CompressionCodecName.GZIP,
                false,
                saveAsFile,
                false);
        inputOutput.payload().save(saveArgs, false);
    }

    @Override
    public boolean exists(final Sample sample, final OutputType type) {
        try {
            return fileSystem.exists(new Path(dataLocation.uri(type, sample)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
