package com.hartwig.pipeline.adam;

import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;
import org.bdgenomics.formats.avro.AlignmentRecord;

public class UnmappedReads {

    private final JavaRDD<AlignmentRecord> theUnmapped;

    private UnmappedReads(final JavaRDD<AlignmentRecord> theUnmapped) {
        this.theUnmapped = theUnmapped;
    }

    AlignmentRecordDataset toAlignment(AlignmentRecordDataset original) {
        return original.replaceRdd(original.rdd().union(theUnmapped.rdd()), original.optPartitionMap());
    }

    public static UnmappedReads from(final AlignmentRecordDataset AlignmentRecordDataset) {
        return new UnmappedReads(AlignmentRecordDataset.rdd().toJavaRDD().filter(read -> !read.getReadMapped()));
    }
}
