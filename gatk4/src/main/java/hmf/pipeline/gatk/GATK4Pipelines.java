package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.patient.Reference;
import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;

public class GATK4Pipelines {

    public static Pipeline<ReadsAndHeader> preProcessing(final Configuration configuration, final JavaSparkContext context) {
        return Pipeline.<ReadsAndHeader>builder().preProcessor(new GATKPreProcessor(Reference.from(configuration), context))
                .perSampleStore(new GATKSampleStore(context))
                .build();
    }
}
