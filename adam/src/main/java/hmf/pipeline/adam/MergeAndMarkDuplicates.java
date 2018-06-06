package hmf.pipeline.adam;

import java.io.IOException;

import org.bdgenomics.adam.api.java.JavaADAMContext;

import hmf.io.PipelineOutput;
import hmf.pipeline.Stage;
import hmf.sample.FlowCell;

public class MergeAndMarkDuplicates implements Stage<FlowCell> {

    private final JavaADAMContext javaADAMContext;

    MergeAndMarkDuplicates(final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.DUPLICATE_MARKED;
    }

    @Override
    public void execute(final FlowCell input) {
        javaADAMContext.loadAlignments("/Users/pwolfe/Code/pipeline2/system/results/TESTX_H7YRLADXX_S1_L00*_aligned.bam")
                .markDuplicates()
                .save(Persistence.defaultSave(input, output()), true);
    }
}
