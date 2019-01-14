package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class MarkDups implements Stage<AlignmentRecordRDD, AlignmentRecordRDD> {

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }

    @Override
    public InputOutput<AlignmentRecordRDD> execute(final InputOutput<AlignmentRecordRDD> input) {
        return InputOutput.of(OutputType.DUPLICATE_MARKED, input.sample(), input.payload().markDuplicates());
    }
}
