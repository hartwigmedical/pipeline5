package hmf.pipeline.gatk;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;

import hmf.io.Output;
import hmf.io.OutputFile;
import hmf.io.OutputStore;
import hmf.patient.Sample;

class GATKSampleStore implements OutputStore<Sample, ReadsAndHeader> {

    private final JavaSparkContext javaSparkContext;

    GATKSampleStore(final JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
    }

    @Override
    public void store(final Output<Sample, ReadsAndHeader> output) {
        try {
            ReadsSparkSink.writeReads(javaSparkContext,
                    OutputFile.of(output.type(), output.entity()).path(),
                    null,
                    output.payload().reads(),
                    output.payload().header(),
                    ReadsWriteFormat.SINGLE);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write GATK reads to filesystem", e);
        }
    }
}
