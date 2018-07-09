package com.hartwig.pipeline.adam;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.rdd.GenomicDataset;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class RDDs {

    static AlignmentRecordRDD persist(AlignmentRecordRDD unpersisted) {
        //noinspection RedundantCast
        return (AlignmentRecordRDD) unpersisted.persist(StorageLevel.DISK_ONLY());
    }

    static AlignmentRecordRDD alignmentRecordRDD(GenomicDataset genomicDataset) {
        return (AlignmentRecordRDD) genomicDataset;
    }
}
