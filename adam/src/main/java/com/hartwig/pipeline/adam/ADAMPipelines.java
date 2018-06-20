package com.hartwig.pipeline.adam;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Configuration;
import com.hartwig.pipeline.Pipeline;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD> preProcessing(Configuration configuration, ADAMContext adamContext) {
        return Pipeline.<AlignmentRecordRDD>builder().addPreProcessingStage(new ADAMBwa(ReferenceGenome.from(configuration), adamContext))
                .addPreProcessingStage(new ADAMMarkDuplicates())
                .perSampleStore(new ADAMSampleStore())
                .build();
    }
}
