package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;

public class MarkDups implements Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) {
        return InputOutput.of(OutputType.DUPLICATE_MARKED, input.sample(), input.payload().markDuplicates());
    }
}
