package com.hartwig.pipeline.adam;

import com.hartwig.io.OutputFile;
import com.hartwig.io.OutputType;
import com.hartwig.patient.FileSystemEntity;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.bdgenomics.adam.rdd.JavaSaveArgs;

class Persistence {

    static JavaSaveArgs defaultSave(FileSystemEntity hasSample, OutputType output) {
        return new JavaSaveArgs(OutputFile.of(output, hasSample).path(), 64, 64, CompressionCodecName.UNCOMPRESSED, false, true, false);
    }
}
