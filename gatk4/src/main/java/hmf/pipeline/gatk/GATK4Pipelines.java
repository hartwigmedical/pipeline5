package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Pipeline;
import hmf.sample.Reference;

public class GATK4Pipelines {

    public static Pipeline sortedAligned(Reference reference, JavaSparkContext context) {
        return Pipeline.builder()
                .addLaneStage(GATK4Stages.ubamFromFastQ())
                .addLaneStage(GATK4Stages.bwaSpark(reference, context))
                .addLaneStage(GATK4Stages.coordinateSortSAM())
                .build();
    }
}
