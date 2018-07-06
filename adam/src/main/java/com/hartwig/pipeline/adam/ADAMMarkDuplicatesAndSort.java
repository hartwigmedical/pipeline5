package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicatesAndSort implements Stage<Sample, AlignmentRecordRDD, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMMarkDuplicatesAndSort(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.ALIGNED, javaADAMContext);
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(outputType(),
                input.entity(),
                ((AlignmentRecordRDD) input.payload()
                        .markDuplicates()
                        .sortReadsByReferencePositionAndIndex()
                        .persist(StorageLevel.DISK_ONLY())));
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
