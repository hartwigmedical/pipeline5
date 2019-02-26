package com.hartwig.pipeline.adam;

import java.util.Arrays;
import java.util.stream.Collector;

import com.hartwig.io.InputOutput;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Stage;

import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.algorithms.consensus.ConsensusGeneratorFromKnowns;
import org.bdgenomics.adam.algorithms.consensus.ConsensusGeneratorFromReads;
import org.bdgenomics.adam.algorithms.consensus.UnionConsensusGenerator;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.variant.VariantDataset;
import org.bdgenomics.adam.util.ReferenceFile;
import org.bdgenomics.formats.avro.Variant;
import org.jetbrains.annotations.NotNull;

import scala.Option;
import scala.collection.JavaConversions;

public class MarkDupsAndRealignIndels implements Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    private final KnownIndels knownIndels;
    private final ReferenceGenome referenceGenome;
    private final JavaADAMContext javaADAMContext;

    MarkDupsAndRealignIndels(final KnownIndels knownIndels, final ReferenceGenome referenceGenome, final JavaADAMContext javaADAMContext) {
        this.knownIndels = knownIndels;
        this.referenceGenome = referenceGenome;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) {
        RDD<Variant> allKnownVariants = knownIndels.paths().stream().map(javaADAMContext::loadVariants).map(VariantDataset::rdd)
                .collect(Collector.of(() -> javaADAMContext.getSparkContext().<Variant>emptyRDD().rdd(), RDD::union, RDD::union));
        ReferenceFile fasta = javaADAMContext.loadReferenceFile(referenceGenome.path());
        UnmappedReads unmapped = UnmappedReads.from(input.payload());
        return InputOutput.of(input.sample(), unmapped.toAlignment(input.payload().markDuplicates()
                // pawo: set realignment parameters to ADAM defaults (see scala) which also map to GATK defaults
                .realignIndels(consensusFromKnownsAndReads(allKnownVariants),
                        false,
                        500, 30, 5.0, 3000, 20000, false, Option.apply(fasta))));
    }

    @NotNull
    private static UnionConsensusGenerator consensusFromKnownsAndReads(final RDD<Variant> allKnownVariants) {
        return new UnionConsensusGenerator(JavaConversions.asScalaBuffer(Arrays.asList(new ConsensusGeneratorFromKnowns(allKnownVariants,
                0), new ConsensusGeneratorFromReads())));
    }
}
