package hmf.pipeline.adam;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

import hmf.patient.Reference;
import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;

public class ADAMPipelines {

    public static Pipeline<AlignmentRecordRDD> preProcessing(Configuration configuration, ADAMContext adamContext) {
        return Pipeline.<AlignmentRecordRDD>builder().preProcessor(new ADAMPreProcessor(Reference.from(configuration), adamContext))
                .perSampleStore(new ADAMSampleStore())
                .build();
    }
}
