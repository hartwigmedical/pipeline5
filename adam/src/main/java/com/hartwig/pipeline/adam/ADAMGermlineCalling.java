package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantContextRDD;
import org.bdgenomics.avocado.genotyping.BiallelicGenotyper;
import org.bdgenomics.avocado.models.CopyNumberMap;

import scala.Option;

public class ADAMGermlineCalling implements Stage<Sample, AlignmentRecordRDD, VariantContextRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMGermlineCalling(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.INDEL_REALIGNED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.GERMLINE_VARIANTS;
    }

    @Override
    public InputOutput<Sample, VariantContextRDD> execute(final InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(outputType(),
                input.entity(),
                BiallelicGenotyper.discoverAndCall(input.payload(),
                        CopyNumberMap.empty(2),
                        false,
                        Option.empty(),
                        Option.empty(),
                        Option.empty(),
                        Option.empty(),
                        Option.empty(),
                        93,
                        93).toVariantContexts());
    }
}
