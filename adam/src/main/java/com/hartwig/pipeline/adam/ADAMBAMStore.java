package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMBAMStore implements OutputStore<AlignmentRecordRDD> {

    @Override
    public void store(final InputOutput<AlignmentRecordRDD> inputOutput) {
        inputOutput.payload().save(Persistence.defaultSave(inputOutput.sample(), inputOutput.type()), true);
    }
}
