package com.hartwig.pipeline.adam;

import com.hartwig.io.DataLocation;
import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;

public class HDFSAlignmentRDDSource implements DataSource<AlignmentRecordDataset> {

    private final JavaADAMContext javaADAMContext;
    private final DataLocation dataLocation;

    HDFSAlignmentRDDSource(final JavaADAMContext javaADAMContext, final DataLocation dataLocation) {
        this.javaADAMContext = javaADAMContext;
        this.dataLocation = dataLocation;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> extract(final Sample sample) {
        return InputOutput.of(sample, javaADAMContext.loadAlignments(dataLocation.uri(sample, "")));
    }
}
