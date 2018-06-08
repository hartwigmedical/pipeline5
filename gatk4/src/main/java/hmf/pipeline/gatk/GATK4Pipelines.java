package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.patient.Reference;
import hmf.pipeline.Pipeline;

public class GATK4Pipelines {

    public static Pipeline<ReadsAndHeader> preProcessing(final Reference reference, final JavaSparkContext context) {
        return Pipeline.<ReadsAndHeader>builder().preProcessor(new GATKPreProcessor(reference, context))
                .perSampleStore(new GATKSampleStore(context))
                .build();
    }
}
