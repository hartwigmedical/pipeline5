package com.hartwig.pipeline.adam;

import com.hartwig.io.Output;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMSampleStore implements OutputStore<Sample, AlignmentRecordRDD> {

    @Override
    public void store(final Output<Sample, AlignmentRecordRDD> output) {
        output.payload().save(Persistence.defaultSave(output.entity(), output.type()), true);
    }
}
