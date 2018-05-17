package hmf.pipeline.adam;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;

public class ADAMPipelines {

    public static Pipeline sortedAligned(Configuration configuration, ADAMContext adamContext) {
        return Pipeline.builder()
                .addStage(ADAMStages.bwaPipe(configuration, adamContext))
                .addStage(ADAMStages.coordinateSort(configuration, new JavaADAMContext(adamContext)))
                .build();
    }
}
