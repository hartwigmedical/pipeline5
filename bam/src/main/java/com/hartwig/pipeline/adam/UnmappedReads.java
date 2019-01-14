package com.hartwig.pipeline.adam;

import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.formats.avro.AlignmentRecord;

public class UnmappedReads {

    private final JavaRDD<AlignmentRecord> unmapped;

    private UnmappedReads(final JavaRDD<AlignmentRecord> unmapped) {
        this.unmapped = unmapped;
    }

    AlignmentRecordRDD toAlignment(AlignmentRecordRDD original) {
        return original.replaceRdd(original.rdd().union(unmapped.rdd()), original.optPartitionMap());
    }

    public static UnmappedReads from(final AlignmentRecordRDD AlignmentRecordRDD) {
        return new UnmappedReads(AlignmentRecordRDD.rdd().toJavaRDD().filter(read -> !read.getReadMapped()));
    }
}
