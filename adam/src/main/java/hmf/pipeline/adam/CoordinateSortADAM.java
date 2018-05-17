package hmf.pipeline.adam;

import java.io.IOException;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs;

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;
import hmf.pipeline.Stage;

class CoordinateSortADAM implements Stage {

    private final JavaADAMContext javaADAMContext;
    private final Configuration configuration;

    CoordinateSortADAM(final Configuration configuration, final JavaADAMContext javaADAMContext) {
        this.javaADAMContext = javaADAMContext;
        this.configuration = configuration;
    }

    @Override
    public PipelineOutput output() {
        return PipelineOutput.SORTED;
    }

    @Override
    public void execute() throws IOException {
        ADAMSaveAnyArgs args = SaveArgs.defaultSave(configuration, output());
        javaADAMContext.loadAlignments(PipelineOutput.ALIGNED.path(configuration.sampleName())).save(args, true);
    }
}
