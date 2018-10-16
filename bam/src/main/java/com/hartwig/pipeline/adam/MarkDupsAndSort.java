package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class MarkDupsAndSort implements Stage<AlignmentRecordRDD, AlignmentRecordRDD> {

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }

    @Override
    public InputOutput<AlignmentRecordRDD> execute(final InputOutput<AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(OutputType.DUPLICATE_MARKED, input.sample(), input.payload().markDuplicates());
    }
}
