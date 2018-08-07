package com.hartwig.pipeline.adam;

import static htsjdk.samtools.SAMRecord.getReadPositionAtReferencePosition;
import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.models.Coverage;
import org.jetbrains.annotations.NotNull;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;
import scala.Tuple2;

class CoverageRDD {

    static JavaRDD<Coverage> toCoverage(final String contig, final RDD<SAMRecordWritable> samRecordRDD) {
        return samRecordRDD.toJavaRDD()
                .map(SAMRecordWritable::get)
                .mapToPair(record -> Tuple2.apply(record.getReadName(), record))
                .groupByKey()
                .flatMap(toOverlappingPositionsWithMinimumBases())
                .mapToPair(index -> Tuple2.apply(index, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .map(tuple -> new Coverage(contig, tuple._1, tuple._1 + 1, tuple._2));
    }

    @NotNull
    private static FlatMapFunction<Tuple2<String, Iterable<SAMRecord>>, Integer> toOverlappingPositionsWithMinimumBases() {
        return tuple -> {
            Set<Integer> positions = new HashSet<>();
            for (SAMRecord record : tuple._2) {
                for (int index = record.getStart(); index < record.getEnd(); index++) {
                    if (baseQualityAtLeastTen(record, index)) {
                        positions.add(index);
                    }
                }
            }
            return positions.iterator();
        };
    }

    private static boolean baseQualityAtLeastTen(final SAMRecord record, final int index) {
        int readPosition = getReadPositionAtReferencePosition(record, index, false);
        return readPosition != 0 && fastqToPhred(record.getBaseQualityString().charAt(readPosition)) >= 10;
    }
}
