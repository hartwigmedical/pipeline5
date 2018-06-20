package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicates implements Stage<Sample, AlignmentRecordRDD> {

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(InputOutput<Sample, AlignmentRecordRDD> chainedInputOutput) throws IOException {
        return InputOutput.of(outputType(), chainedInputOutput.entity(), chainedInputOutput.payload().markDuplicates());
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
