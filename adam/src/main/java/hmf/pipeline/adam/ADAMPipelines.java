package hmf.pipeline.adam;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

import hmf.pipeline.Pipeline;
import hmf.sample.Reference;

public class ADAMPipelines {

    public static Pipeline sortedAligned(Reference reference, ADAMContext adamContext) {
        return Pipeline.builder()
                .addLaneStage(ADAMStages.bwaPipe(reference, adamContext))
                .addLaneStage(ADAMStages.coordinateSort(new JavaADAMContext(adamContext)))
                .build();
    }

    public static Pipeline sortedAlignedDupsMarked(Reference reference, ADAMContext adamContext) {
        JavaADAMContext javaADAMContext = new JavaADAMContext(adamContext);
        return Pipeline.builder()
                .addLaneStage(ADAMStages.bwaPipe(reference, adamContext))
                .addLaneStage(ADAMStages.coordinateSort(javaADAMContext))
                .setMergeStage(ADAMStages.mergeAndMarkDuplicates(javaADAMContext))
                .build();
    }
}
