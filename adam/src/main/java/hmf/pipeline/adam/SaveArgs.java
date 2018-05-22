package hmf.pipeline.adam;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

import hmf.pipeline.PipelineOutput;
import hmf.sample.Lane;

class SaveArgs {

    static JavaSaveArgs defaultSave(Lane lane, PipelineOutput output) {
        return new JavaSaveArgs(output.path(lane), 0, 0, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
