package hmf.pipeline.adam;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

import hmf.io.Output;
import hmf.io.OutputStore;
import hmf.patient.Sample;

public class ADAMSampleStore implements OutputStore<Sample, AlignmentRecordRDD> {

    @Override
    public void store(final Output<Sample, AlignmentRecordRDD> output) {
        output.payload().save(Persistence.defaultSave(output.entity(), output.type()), true);
    }
}
