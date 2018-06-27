package com.hartwig.pipeline.adam;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class AlignmentRDDSource implements DataSource<Sample, AlignmentRecordRDD> {

    private final OutputType outputType;
    private final JavaADAMContext javaADAMContext;

    public AlignmentRDDSource(final OutputType outputType, final JavaADAMContext javaADAMContext) {
        this.outputType = outputType;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> extract(final Sample entity) {
        return InputOutput.of(outputType, entity, javaADAMContext.loadAlignments(OutputFile.of(outputType, entity).path()));
    }
}
