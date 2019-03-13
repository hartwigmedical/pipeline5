package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataLocation;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSBamStore implements OutputStore<AlignmentRecordDataset> {

    private final static Logger LOGGER = LoggerFactory.getLogger(OutputStore.class);

    private final DataLocation dataLocation;
    private final FileSystem fileSystem;
    private final boolean mergeFinalFile;

    HDFSBamStore(final DataLocation dataLocation, final FileSystem fileSystem, final boolean saveAsFile) {
        this.dataLocation = dataLocation;
        this.fileSystem = fileSystem;
        this.mergeFinalFile = saveAsFile;
    }

    @Override
    public void store(final InputOutput<AlignmentRecordDataset> inputOutput) {
        store(inputOutput, "");
    }

    @Override
    public void store(final InputOutput<AlignmentRecordDataset> inputOutput, String suffix) {
        String storageUri = dataLocation.uri(inputOutput.sample(), suffix);
        LOGGER.info("persisting to... {}", storageUri);
        JavaSaveArgs saveArgs = new JavaSaveArgs(storageUri,
                128 * 1024 * 1024,
                1024 * 1024,
                CompressionCodecName.GZIP,
                false,
                true,
                false);
        saveArgs.deferMerging_$eq(!mergeFinalFile);
        inputOutput.payload().save(saveArgs, true);
    }

    @Override
    public boolean exists(final Sample sample) {
        try {
            return fileSystem.exists(new Path(dataLocation.uri(sample, "")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        try {
            fileSystem.delete(new Path(dataLocation.rootUri()), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
