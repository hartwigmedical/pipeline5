package com.hartwig.pipeline.adam;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.io.InputOutput;
import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.KnownSnps;
import com.hartwig.pipeline.Stage;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.adam.rdd.variant.VariantDataset;

import scala.collection.JavaConversions;

public class BaseQualityScoreRecalibration implements Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    private static final int GATK_DEFAULT_MIN_QUALITY = 6;
    private final KnownSnps knownSnps;
    private final KnownIndels knownIndels;
    private final JavaADAMContext javaADAMContext;

    BaseQualityScoreRecalibration(final KnownSnps knownSnps, final KnownIndels knownIndels, final JavaADAMContext javaADAMContext) {
        this.knownSnps = knownSnps;
        this.knownIndels = knownIndels;
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public InputOutput<AlignmentRecordDataset> execute(final InputOutput<AlignmentRecordDataset> input) {
        if (knownIndels.paths().isEmpty() && knownSnps.paths().isEmpty()) {
            throw new IllegalArgumentException("Cannot run base quality recalibration with no known snps or indels. "
                    + "Check your configuration to ensure at least one of these VCFs is passed to the pipeline.");
        }

        List<VariantDataset> allIndelsAndSnps = Stream.concat(knownIndels.paths().stream(), knownSnps.paths().stream())
                .map(javaADAMContext::loadVariants)
                .collect(Collectors.toList());

        assert allIndelsAndSnps.size() == 2;
        VariantDataset knownVariantDataset =
                allIndelsAndSnps.get(0).union(JavaConversions.asScalaBuffer(allIndelsAndSnps.subList(1, allIndelsAndSnps.size())));

        return InputOutput.of(input.sample(),
                input.payload().recalibrateBaseQualities(knownVariantDataset, GATK_DEFAULT_MIN_QUALITY, StorageLevel.MEMORY_ONLY()));
    }
}
