package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicates implements Stage<Sample, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMMarkDuplicates(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext);
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(outputType(), input.entity(), input.payload().markDuplicates());
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
