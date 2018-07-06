package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.stream.Collector;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Stage;

import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.algorithms.consensus.ConsensusGeneratorFromKnowns;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantRDD;
import org.bdgenomics.formats.avro.Variant;

public class ADAMRealignIndels implements Stage<Sample, AlignmentRecordRDD, AlignmentRecordRDD> {

    private final KnownIndels knownIndels;
    private final JavaADAMContext javaADAMContext;

    ADAMRealignIndels(final KnownIndels knownIndels, final JavaADAMContext javaADAMContext) {
        this.knownIndels = knownIndels;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<Sample, AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.DUPLICATE_MARKED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.INDEL_REALIGNED;
    }

    @Override
    public InputOutput<Sample, AlignmentRecordRDD> execute(final InputOutput<Sample, AlignmentRecordRDD> input) throws IOException {

        RDD<Variant> allKnownVariants = knownIndels.paths()
                .stream()
                .map(javaADAMContext::loadVariants)
                .map(VariantRDD::rdd)
                .collect(Collector.of(() -> javaADAMContext.getSparkContext().<Variant>emptyRDD().rdd(), RDD::union, RDD::union));

        return InputOutput.of(OutputType.INDEL_REALIGNED,
                input.entity(),
                input.payload().realignIndels(new ConsensusGeneratorFromKnowns(allKnownVariants, 0), true, 500, 30, 5.0, 3000));
    }
}
