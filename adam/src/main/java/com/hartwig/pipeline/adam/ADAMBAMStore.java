package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMBAMStore implements OutputStore<Sample, AlignmentRecordRDD> {

    @Override
    public void store(final InputOutput<Sample, AlignmentRecordRDD> inputOutput) {
        inputOutput.payload().save(Persistence.defaultSave(inputOutput.entity(), inputOutput.type()), true);
    }
}
