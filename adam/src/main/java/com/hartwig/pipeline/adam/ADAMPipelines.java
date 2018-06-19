package com.hartwig.pipeline.adam;

import com.hartwig.patient.Reference;
import com.hartwig.pipeline.Configuration;
import com.hartwig.pipeline.Pipeline;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD> preProcessing(Configuration configuration, ADAMContext adamContext) {
        return Pipeline.<AlignmentRecordRDD>builder().preProcessor(new ADAMPreProcessor(Reference.from(configuration), adamContext))
                .perSampleStore(new ADAMSampleStore())
                .build();
    }
}
