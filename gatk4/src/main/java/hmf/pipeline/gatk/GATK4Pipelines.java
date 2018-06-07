package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Pipeline;
import hmf.sample.Reference;

public class GATK4Pipelines {

    public static Pipeline preProcessing(final Reference reference, final JavaSparkContext context) {
        return Pipeline.builder().preProcessor(new GATKPreProcessor(reference, context)).build();
    }
}
