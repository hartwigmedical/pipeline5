package hmf.pipeline.adam;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.sample.Lane;

class Persistence {

    static JavaSaveArgs defaultSave(Lane lane, PipelineOutput output) {
        return new JavaSaveArgs(OutputFile.of(output, lane).path(), 0, 0, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
