package com.hartwig.pipeline.adam;

import com.hartwig.io.DataLocation;
import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class HDFSAlignmentRDDSource implements DataSource<AlignmentRecordRDD> {

    private final OutputType outputType;
    private final JavaADAMContext javaADAMContext;
    private final DataLocation dataLocation;

    HDFSAlignmentRDDSource(final OutputType outputType, final JavaADAMContext javaADAMContext, final DataLocation dataLocation) {
        this.outputType = outputType;
        this.javaADAMContext = javaADAMContext;
        this.dataLocation = dataLocation;
    }

    @Override
    public InputOutput<AlignmentRecordRDD> extract(final Sample sample) {
        return InputOutput.of(outputType, sample, javaADAMContext.loadAlignments(dataLocation.uri(outputType, sample)));
    }
}
