package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Pipeline;
import hmf.sample.Reference;

public class GATK4Pipelines {

    public static Pipeline sortedAligned(Reference reference, JavaSparkContext context) {
        return sortedAlignedBuilder(reference, context).build();
    }

    public static Pipeline sortedAlignedDupsMarked(Reference reference, JavaSparkContext context) {
        return sortedAlignedBuilder(reference, context).setMergeStage(GATK4Stages.markDuplicates()).build();
    }

    private static Pipeline.Builder sortedAlignedBuilder(final Reference reference, final JavaSparkContext context) {
        return Pipeline.builder()
                .addLaneStage(GATK4Stages.ubamFromFastQ())
                .addLaneStage(GATK4Stages.bwaSpark(reference, context))
                .addLaneStage(GATK4Stages.coordinateSortSAM());
    }
}
