package com.hartwig.pipeline.adam;

import static htsjdk.samtools.SAMRecord.getReadPositionAtReferencePosition;
import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.models.Coverage;
import org.jetbrains.annotations.NotNull;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;

class CoverageRDD {

    static JavaRDD<Coverage> toCoverage(String contig, final RDD<SAMRecordWritable> samRecordRDD) {
        return samRecordRDD.toJavaRDD()
                .map(SAMRecordWritable::get).filter(record -> record.getReferenceName().equals(contig))
                .flatMap(window -> LongStream.range(window.getStart(), window.getEnd() - 1)
                        .boxed().filter(baseQualityAtLeastTen(window))
                        .collect(Collectors.toList())
                        .iterator())
                .keyBy(index -> index)
                .groupByKey().map(tuple -> new Coverage(contig,
                        tuple._1,
                        tuple._1 + 1,
                        StreamSupport.stream(tuple._2.spliterator(), false).count()));
    }

    @NotNull
    private static Predicate<Long> baseQualityAtLeastTen(final SAMRecord record) {
        return index -> {
            int readPosition = getReadPositionAtReferencePosition(record, index.intValue(), false);
            return readPosition != 0 && fastqToPhred(record.getBaseQualityString().charAt(readPosition)) >= 10;
        };
    }
}
