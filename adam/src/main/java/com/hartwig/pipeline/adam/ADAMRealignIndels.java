package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.algorithms.consensus.ConsensusGeneratorFromKnowns;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMRealignIndels implements Stage<Sample, AlignmentRecordRDD> {

    private final JavaADAMContext javaADAMContext;

    public ADAMRealignIndels(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.DUPLICATE_MARKED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.REALIGNED;
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(final InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(OutputType.REALIGNED,
                input.entity(),
                input.payload()
                        .realignIndels(new ConsensusGeneratorFromKnowns(javaADAMContext.loadVariants("/path/to/knowns").rdd(), 0),
                                true,
                                500,
                                30,
                                5.0,
                                3000));
    }
}
