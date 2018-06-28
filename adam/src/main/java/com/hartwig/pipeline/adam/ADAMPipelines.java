package com.hartwig.pipeline.adam;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Pipeline;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD> preProcessing(String referenceGenomePath, ADAMContext adamContext, int bwaThreads) {
        return Pipeline.<AlignmentRecordRDD>builder().addPreProcessingStage(new ADAMBwa(ReferenceGenome.from(referenceGenomePath),
                adamContext,
                bwaThreads))
                .addPreProcessingStage(new ADAMMarkDuplicates(new JavaADAMContext(adamContext)))
                .perSampleStore(new ADAMSampleStore())
                .build();
    }
}
