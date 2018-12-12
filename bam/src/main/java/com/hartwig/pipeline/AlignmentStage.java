package com.hartwig.pipeline;

import com.hartwig.io.OutputType;

import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;

public interface AlignmentStage extends Stage<AlignmentRecordDataset, AlignmentRecordDataset> {

    @Override
    default OutputType outputType() {
        return OutputType.ALIGNED;
    }
}
