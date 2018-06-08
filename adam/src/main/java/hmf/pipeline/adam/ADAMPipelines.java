package hmf.pipeline.adam;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

import hmf.patient.Reference;
import hmf.pipeline.Pipeline;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD> preProcessing(Reference reference, ADAMContext adamContext) {
        return Pipeline.<AlignmentRecordRDD>builder().preProcessor(new ADAMPreProcessor(reference, adamContext))
                .perSampleStore(new ADAMSampleStore())
                .build();
    }
}
