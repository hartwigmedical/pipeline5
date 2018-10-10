package com.hartwig.pipeline.adam;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.rdd.GenomicDataset;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class RDDs {

    static AlignmentRecordRDD persistDisk(AlignmentRecordRDD unpersisted) {
        return persistTo(unpersisted, StorageLevel.DISK_ONLY());
    }

    private static AlignmentRecordRDD persistTo(final AlignmentRecordRDD unpersisted, final StorageLevel storageLevel) {
        //noinspection RedundantCast
        return (AlignmentRecordRDD) unpersisted.persist(storageLevel);
    }

    static AlignmentRecordRDD alignmentRecordRDD(GenomicDataset genomicDataset) {
        return (AlignmentRecordRDD) genomicDataset;
    }
}
