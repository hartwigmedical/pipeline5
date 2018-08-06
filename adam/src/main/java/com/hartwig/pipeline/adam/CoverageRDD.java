package com.hartwig.pipeline.adam;

import static htsjdk.samtools.SAMRecord.getReadPositionAtReferencePosition;
import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.api.java.JavaRDD;
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
                .flatMapToPair(record -> LongStream.range(record.getStart(), record.getEnd() - 1)
                        .boxed()
                        .filter(baseQualityAtLeastTen(record))
                        .map(index -> Tuple2.apply(record.getReadName(), index))
                        .collect(Collectors.toList())
                        .iterator())
                .distinct()
                .values()
                .mapToPair(index -> Tuple2.apply(index, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .map(tuple -> new Coverage(contig, tuple._1, tuple._1 + 1, tuple._2));
    }

    @NotNull
    private static Predicate<Long> baseQualityAtLeastTen(final SAMRecord record) {
        return index -> {
            int readPosition = getReadPositionAtReferencePosition(record, index.intValue(), false);
            return readPosition != 0 && fastqToPhred(record.getBaseQualityString().charAt(readPosition)) >= 10;
        };
    }
}
