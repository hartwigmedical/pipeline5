package hmf.pipeline.adam;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.sample.HasSample;

class Persistence {

    static JavaSaveArgs defaultSave(HasSample hasSample, PipelineOutput output) {
        return new JavaSaveArgs(OutputFile.of(output, hasSample).path(), 0, 0, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
