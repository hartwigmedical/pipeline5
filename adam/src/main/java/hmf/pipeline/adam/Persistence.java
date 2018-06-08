package hmf.pipeline.adam;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

import hmf.io.OutputFile;
import hmf.io.PipelineOutput;
import hmf.patient.FileSystemEntity;

class Persistence {

    static JavaSaveArgs defaultSave(FileSystemEntity hasSample, PipelineOutput output) {
        return new JavaSaveArgs(OutputFile.of(output, hasSample).path(), 64, 64, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
