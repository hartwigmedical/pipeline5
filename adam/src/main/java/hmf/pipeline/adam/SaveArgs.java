package hmf.pipeline.adam;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

import hmf.pipeline.Configuration;
import hmf.pipeline.PipelineOutput;

class SaveArgs {

    static JavaSaveArgs defaultSave(Configuration configuration, PipelineOutput output) {
        return new JavaSaveArgs(output.path(configuration.sampleName()), 0, 0, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
