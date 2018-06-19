package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.Output;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class ADAMMarkDuplicates implements Stage<Sample, AlignmentRecordRDD> {

    private final JavaADAMContext adamContext;

    ADAMMarkDuplicates(final JavaADAMContext adamContext) {
        this.adamContext = adamContext;
    }

    @Override
    public Output<Sample, AlignmentRecordRDD> execute(Sample sample) throws IOException {
        return Output.of(outputType(),
                sample,
                adamContext.loadAlignments(OutputFile.of(OutputType.ALIGNED, sample).path()).markDuplicates());
    }

    @Override
    public OutputType outputType() {
        return OutputType.DUPLICATE_MARKED;
    }
}
