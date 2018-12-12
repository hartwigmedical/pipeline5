package com.hartwig.pipeline.adam;

import org.apache.spark.storage.StorageLevel;
import org.bdgenomics.adam.rdd.GenomicDataset;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;

class RDDs {

    static AlignmentRecordDataset persistDisk(AlignmentRecordDataset unpersisted) {
        return persistTo(unpersisted, StorageLevel.DISK_ONLY());
    }

    private static AlignmentRecordDataset persistTo(final AlignmentRecordDataset unpersisted, final StorageLevel storageLevel) {
        //noinspection RedundantCast
        return (AlignmentRecordDataset) unpersisted.persist(storageLevel);
    }

    static AlignmentRecordDataset AlignmentRecordDataset(GenomicDataset genomicDataset) {
        return (AlignmentRecordDataset) genomicDataset;
    }
}
