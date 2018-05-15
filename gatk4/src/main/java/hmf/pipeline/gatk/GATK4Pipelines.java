package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;

public class GATK4Pipelines {

    public static Pipeline sortedAligned(Configuration configuration, JavaSparkContext context) {
        return Pipeline.builder()
                .addStage(GATK4Stages.ubamFromFastQ(configuration))
                .addStage(GATK4Stages.bwaSpark(configuration, context))
                .addStage(GATK4Stages.coordinateSortSAM(configuration))
                .build();
    }
}
