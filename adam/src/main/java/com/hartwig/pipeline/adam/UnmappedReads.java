package com.hartwig.pipeline.adam;

import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.formats.avro.AlignmentRecord;

public class UnmappedReads {

    private final JavaRDD<AlignmentRecord> theUnmapped;

    private UnmappedReads(final JavaRDD<AlignmentRecord> theUnmapped) {
        this.theUnmapped = theUnmapped;
    }

    AlignmentRecordRDD toAlignment(AlignmentRecordRDD original) {
        return original.replaceRdd(original.rdd().union(theUnmapped.rdd()), original.optPartitionMap());
    }

    public static UnmappedReads from(final AlignmentRecordRDD alignmentRecordRDD) {
        return new UnmappedReads(alignmentRecordRDD.rdd().toJavaRDD().filter(read -> !read.getReadMapped()));
    }
}
