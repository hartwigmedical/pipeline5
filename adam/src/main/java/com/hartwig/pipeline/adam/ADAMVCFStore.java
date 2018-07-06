package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;
import com.hartwig.patient.Sample;

import org.bdgenomics.adam.rdd.variant.VariantContextRDD;

import htsjdk.samtools.ValidationStringency;

public class ADAMVCFStore implements OutputStore<Sample, VariantContextRDD> {

    @Override
    public void store(final InputOutput<Sample, VariantContextRDD> inputOutput) {
        inputOutput.payload()
                .saveAsVcf(Persistence.defaultSave(inputOutput.entity(), inputOutput.type()), ValidationStringency.DEFAULT_STRINGENCY);
    }
}
