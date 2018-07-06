package com.hartwig.pipeline.adam;

import java.util.List;

import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Pipeline;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantContextRDD;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD, VariantContextRDD> preProcessing(String referenceGenomePath, List<String> knownIndelPaths,
            ADAMContext adamContext, int bwaThreads, boolean callGermline) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        Pipeline.Builder<AlignmentRecordRDD, VariantContextRDD> builder =
                Pipeline.<AlignmentRecordRDD, VariantContextRDD>builder().addPreProcessingStage(new ADAMBwa(ReferenceGenome.from(
                        referenceGenomePath), adamContext, bwaThreads))
                        .addPreProcessingStage(new ADAMMarkDuplicatesAndSort(javaADAMContext));
        if (!knownIndelPaths.isEmpty()) {
            builder.addPreProcessingStage(new ADAMRealignIndels(KnownIndels.of(knownIndelPaths), javaADAMContext));
        }
        if (callGermline) {
            builder.germlineCalling(new ADAMGermlineCalling(javaADAMContext)).vcfStore(new ADAMVCFStore());
        }
        return builder.bamStore(new ADAMBAMStore()).build();
    }
}
