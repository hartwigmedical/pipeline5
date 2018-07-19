package com.hartwig.pipeline.adam;

import java.io.IOException;

import com.hartwig.io.DataSource;
import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputType;
import com.hartwig.pipeline.Stage;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantContextRDD;
import org.bdgenomics.avocado.genotyping.BiallelicGenotyper;
import org.bdgenomics.avocado.models.CopyNumberMap;
import org.bdgenomics.avocado.util.PrefilterReads;
import org.bdgenomics.avocado.util.PrefilterReadsArgs;

import scala.Option;

public class ADAMGermlineCalling implements Stage<AlignmentRecordRDD, VariantContextRDD> {

    private final JavaADAMContext javaADAMContext;

    ADAMGermlineCalling(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public DataSource<AlignmentRecordRDD> datasource() {
        return new AlignmentRDDSource(OutputType.MD_TAGGED, javaADAMContext);
    }

    @Override
    public OutputType outputType() {
        return OutputType.GERMLINE_VARIANTS;
    }

    @Override
    public InputOutput<VariantContextRDD> execute(final InputOutput<AlignmentRecordRDD> input) throws IOException {
        return InputOutput.of(outputType(), input.sample(),
                BiallelicGenotyper.discoverAndCall(RDDs.persistMemoryAndDisk(PrefilterReads.apply(RDDs.persistDisk(input.payload()),
                        defaults())),
                        CopyNumberMap.empty(2),
                        false, Option.empty(), Option.apply(15),
                        Option.empty(),
                        Option.empty(),
                        Option.empty(),
                        93,
                        93).toVariantContexts());
    }

    private static PrefilterReadsArgs defaults() {
        return new PrefilterReadsArgs() {
            @Override
            public boolean keepDuplicates() {
                return false;
            }

            @Override
            public void keepDuplicates_$eq(final boolean keepDuplicates) {

            }

            @Override
            public boolean autosomalOnly() {
                return true;
            }

            @Override
            public void autosomalOnly_$eq(final boolean autosomalOnly) {

            }

            @Override
            public boolean keepMitochondrialChromosome() {
                return false;
            }

            @Override
            public void keepMitochondrialChromosome_$eq(final boolean keepMitochondrialChromosome) {

            }

            @Override
            public boolean keepNonPrimary() {
                return false;
            }

            @Override
            public void keepNonPrimary_$eq(final boolean keepNonPrimary) {

            }

            @Override
            public int minMappingQuality() {
                return 10;
            }

            @Override
            public void minMappingQuality_$eq(final int minMappingQuality) {

            }
        };
    }
}
