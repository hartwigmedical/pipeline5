package hmf.pipeline.adam;

import org.bdgenomics.adam.rdd.ADAMContext;

import hmf.pipeline.Pipeline;
import hmf.sample.Reference;

public class ADAMPipelines {

    public static Pipeline preProcessing(Reference reference, ADAMContext adamContext) {
        return Pipeline.builder().preProcessor(new ADAMPreProcessor(reference, adamContext))
                .build();
    }
}
