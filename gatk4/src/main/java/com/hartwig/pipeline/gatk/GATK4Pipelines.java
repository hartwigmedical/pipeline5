package com.hartwig.pipeline.gatk;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.pipeline.ImmutablePipeline;
import com.hartwig.pipeline.Pipeline;

import org.apache.spark.api.java.JavaSparkContext;

public class GATK4Pipelines {

    public static Pipeline<ReadsAndHeader, ReadsAndHeader> preProcessing(final String referenceGenomePath, final JavaSparkContext context) {
        return ImmutablePipeline.<ReadsAndHeader, ReadsAndHeader>builder().addPreProcessors(new GATK4PreProcessor(ReferenceGenome.of(
                referenceGenomePath), context)).bamStore(new GATKSampleStore(context)).build();
    }
}
