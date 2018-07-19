package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicatesAndSort implements Stage<AlignmentRecordRDD, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMMarkDuplicatesAndSort(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext);
    }

    @Override
    public InputOutput<AlignmentRecordRDD> execute(InputOutput<AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(outputType(), input.sample(),
                RDDs.persistDisk(input.payload().markDuplicates().sortReadsByReferencePositionAndIndex()));
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
