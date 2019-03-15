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

public class IndelRealignment implements Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    private static final int GATK_DEFAULT_MAX_INDEL_SIZE = 500;
    private static final int GATK_DEFAULT_MAX_CONSENSUS_NUMBER = 30;
    private static final double GATK_DEFAULT_LOD_THRESHOLD = 5.0;
    private static final int GATK_DEFAULT_MAX_TARGET_SIZE = 3000;
    private static final int GATK_DEFAULT_MAX_READS_PER_TARGET = 20000;
    private final KnownIndels knownIndels;
    private final ReferenceGenome referenceGenome;
    private final JavaADAMContext javaADAMContext;

    IndelRealignment(final KnownIndels knownIndels, final ReferenceGenome referenceGenome, final JavaADAMContext javaADAMContext) {
        this.knownIndels = knownIndels;
        this.referenceGenome = referenceGenome;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) {
        RDD<Variant> allKnownVariants = knownIndels.paths()
                .stream()
                .map(javaADAMContext::loadVariants)
                .map(VariantDataset::rdd)
                .collect(Collector.of(() -> javaADAMContext.getSparkContext().<Variant>emptyRDD().rdd(), RDD::union, RDD::union));
        ReferenceFile fasta = javaADAMContext.loadReferenceFile(referenceGenome.path());
        UnmappedReads unmapped = UnmappedReads.from(input.payload());
        return InputOutput.of(input.sample(),
                unmapped.toAlignment(input.payload()
                        .realignIndels(consensusFromKnownsAndReads(allKnownVariants),
                                false,
                                GATK_DEFAULT_MAX_INDEL_SIZE,
                                GATK_DEFAULT_MAX_CONSENSUS_NUMBER,
                                GATK_DEFAULT_LOD_THRESHOLD,
                                GATK_DEFAULT_MAX_TARGET_SIZE,
                                GATK_DEFAULT_MAX_READS_PER_TARGET,
                                false,
                                Option.apply(fasta))));
    }

    @NotNull
    private static UnionConsensusGenerator consensusFromKnownsAndReads(final RDD<Variant> allKnownVariants) {
        return new UnionConsensusGenerator(JavaConversions.asScalaBuffer(Arrays.asList(new ConsensusGeneratorFromKnowns(allKnownVariants,
                0), new ConsensusGeneratorFromReads())));
    }
}
