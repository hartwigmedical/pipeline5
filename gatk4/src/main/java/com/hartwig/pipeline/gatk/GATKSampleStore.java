package com.hartwig.pipeline.gatk;

import java.io.File;
import java.io.IOException;

import com.hartwig.io.Output;
import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputStore;
import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;

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

    @Override
    public boolean exists(final Sample entity, final OutputType type) {
        return new File(OutputFile.of(type, entity).path()).exists();
    }
}
