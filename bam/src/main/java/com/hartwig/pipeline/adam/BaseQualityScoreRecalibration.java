package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.hartwig.io.InputOutput;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.KnownSnps;
import com.hartwig.pipeline.Stage;

import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.variant.VariantDataset;
import org.bdgenomics.formats.avro.Variant;

import htsjdk.variant.vcf.VCFHeaderLine;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

public class BaseQualityScoreRecalibration implements Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    private static final int ADAM_DEFAULT_MIN_QUALITY = 5;
    private final KnownSnps knownSnps;
    private final KnownIndels knownIndels;
    private final JavaADAMContext javaADAMContext;

    BaseQualityScoreRecalibration(final KnownSnps knownSnps, final KnownIndels knownIndels, final JavaADAMContext javaADAMContext) {
        this.knownSnps = knownSnps;
        this.knownIndels = knownIndels;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) throws IOException {

        RDD<Variant> allIndelsAndSnps = Stream.concat(knownIndels.paths().stream(), knownSnps.paths().stream())
                .map(javaADAMContext::loadVariants)
                .map(VariantDataset::rdd)
                .collect(Collector.of(() -> javaADAMContext.getSparkContext().<Variant>emptyRDD().rdd(), RDD::union, RDD::union));

        return InputOutput.of(input.sample(),
                input.payload()
                        .recalibrateBaseQualities(VariantDataset.apply(allIndelsAndSnps, input.payload().sequences(), emptySeq()),
                                ADAM_DEFAULT_MIN_QUALITY,
                                StorageLevel.MEMORY_ONLY()));
    }

    private static Buffer<VCFHeaderLine> emptySeq() {
        return JavaConversions.asScalaBuffer(Collections.emptyList());
    }
}
