package com.hartwig.pipeline.adam;

import java.util.List;

import com.hartwig.patient.KnownIndels;
import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.ImmutablePipeline;
import com.hartwig.pipeline.Pipeline;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantContextRDD;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD, VariantContextRDD> preProcessing(final String referenceGenomePath,
            final List<String> knownIndelPaths, final ADAMContext adamContext, final int bwaThreads, final boolean callGermline) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        ReferenceGenome referenceGenome = ReferenceGenome.of(referenceGenomePath);
        ImmutablePipeline.Builder<AlignmentRecordRDD, VariantContextRDD> builder = ImmutablePipeline.builder();
        builder.addPreProcessors(new ADAMBwa(referenceGenome, adamContext, bwaThreads))
                .addPreProcessors(new ADAMMarkDuplicatesAndSort(javaADAMContext))
                .addPreProcessors(new ADAMRealignIndels(KnownIndels.of(knownIndelPaths), referenceGenome, javaADAMContext))
                .addPreProcessors(new ADAMAddMDTags(javaADAMContext, referenceGenome));
        if (callGermline) {
            builder.germlineCalling(new ADAMGermlineCalling(javaADAMContext)).vcfStore(new ADAMVCFStore());
        }
        return builder.bamStore(new ADAMBAMStore()).build();
    }
}
