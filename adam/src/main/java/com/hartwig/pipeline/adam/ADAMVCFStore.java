package com.hartwig.pipeline.adam;

import com.hartwig.io.InputOutput;
import com.hartwig.io.OutputStore;

import org.bdgenomics.adam.rdd.variant.VariantContextRDD;

import htsjdk.samtools.ValidationStringency;

public class ADAMVCFStore implements OutputStore<VariantContextRDD> {

    @Override
    public void store(final InputOutput<VariantContextRDD> inputOutput) {
        inputOutput.payload()
                .saveAsVcf(Persistence.defaultSave(inputOutput.sample(), inputOutput.type()), ValidationStringency.DEFAULT_STRINGENCY);
    }
}
