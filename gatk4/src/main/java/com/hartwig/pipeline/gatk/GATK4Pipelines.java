package com.hartwig.pipeline.gatk;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.Pipeline;

import org.apache.spark.api.java.JavaSparkContext;

public class GATK4Pipelines {

    public static Pipeline<ReadsAndHeader, ReadsAndHeader> preProcessing(final String referenceGenomePath, final JavaSparkContext context) {
        return Pipeline.<ReadsAndHeader, ReadsAndHeader>builder().addPreProcessingStage(new GATK4PreProcessor(ReferenceGenome.from(
                referenceGenomePath), context)).bamStore(new GATKSampleStore(context)).build();
    }
}
