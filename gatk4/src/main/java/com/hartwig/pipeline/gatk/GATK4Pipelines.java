package com.hartwig.pipeline.gatk;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Configuration;
import com.hartwig.pipeline.Pipeline;

import org.apache.spark.api.java.JavaSparkContext;

public class GATK4Pipelines {

    public static Pipeline<ReadsAndHeader> preProcessing(final Configuration configuration, final JavaSparkContext context) {
        return Pipeline.<ReadsAndHeader>builder().addPreProcessingStage(new GATK4PreProcessor(ReferenceGenome.from(configuration), context))
                .perSampleStore(new GATKSampleStore(context))
                .build();
    }
}
