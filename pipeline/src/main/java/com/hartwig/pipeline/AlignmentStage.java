package com.hartwig.pipeline;

import com.hartwig.io.DataSource;
import com.hartwig.io.OutputType;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public interface AlignmentStage extends Stage<AlignmentRecordRDD, AlignmentRecordRDD> {

    @Override
    default DataSource<AlignmentRecordRDD> datasource() {
        return sample -> null;
    }

    @Override
    default OutputType outputType() {
        return OutputType.ALIGNED;
    }
}
