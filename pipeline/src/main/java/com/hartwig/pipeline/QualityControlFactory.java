package com.hartwig.pipeline;

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

public interface QualityControlFactory {

    QualityControl<AlignmentRecordRDD> readCount(AlignmentRecordRDD initial);
}
