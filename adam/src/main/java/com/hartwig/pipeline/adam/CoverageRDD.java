package com.hartwig.pipeline.adam;

import static htsjdk.samtools.SAMRecord.getReadPositionAtReferencePosition;
import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.bdgenomics.adam.models.Coverage;
import org.jetbrains.annotations.NotNull;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;
import scala.Tuple2;

class CoverageRDD {

    static JavaRDD<Coverage> toCoverage(final String contig, final RDD<SAMRecordWritable> samRecordRDD) {
        return samRecordRDD.toJavaRDD().map(SAMRecordWritable::get).flatMapToPair(CoverageRDD::basesWithinRead).reduceByKey((v1, v2) -> v1)
                .values()
                .mapToPair(index -> Tuple2.apply(index, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .map(tuple -> new Coverage(contig, tuple._1, tuple._1 + 1, tuple._2));
    }

    @NotNull
    private static Iterator<Tuple2<Tuple2<Integer, Integer>, Integer>> basesWithinRead(final SAMRecord record) {
        List<Tuple2<Tuple2<Integer, Integer>, Integer>> basesAndReadNames = new ArrayList<>();
        for (int index = record.getStart(); index < record.getEnd(); index++) {
            if (baseQualityAtLeastTen(record, index)) {
                basesAndReadNames.add(Tuple2.apply(Tuple2.apply(record.getReadName().hashCode(), index), index));
            }
        }
        return basesAndReadNames.iterator();
    }

    private static boolean baseQualityAtLeastTen(final SAMRecord record, final int index) {
        int readPosition = getReadPositionAtReferencePosition(record, index, false);
        return readPosition != 0 && fastqToPhred(record.getBaseQualityString().charAt(readPosition)) >= 10;
    }
}
