package com.hartwig.pipeline.adam;

import static htsjdk.samtools.SAMRecord.getReadPositionAtReferencePosition;
import static htsjdk.samtools.SAMUtils.fastqToPhred;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaRDD;
import org.bdgenomics.adam.models.Coverage;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.jetbrains.annotations.NotNull;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;

class CoverageRDD {

    static JavaRDD<Coverage> toCoverage(AlignmentRecordRDD alignmentRecordRDD) {
        return alignmentRecordRDD.convertToSam(false)._1.toJavaRDD().map(SAMRecordWritable::get)
                .flatMap(window -> LongStream.range(window.getStart(), window.getEnd() - 1)
                        .boxed()
                        .filter(baseQualityAtLeastTen(window)).map(index -> new Coverage(window.getContig(), index, index + 1, 1.0))
                        .collect(Collectors.toList())
                        .iterator())
                .keyBy(coverage -> new Position(coverage.contigName(), coverage.start(), coverage.end()))
                .groupByKey()
                .map(tuple -> new Coverage(tuple._1.contigName,
                        tuple._1.start,
                        tuple._1.end,
                        StreamSupport.stream(tuple._2.spliterator(), false).mapToDouble(Coverage::count).sum()));
    }

    @NotNull
    private static Predicate<Long> baseQualityAtLeastTen(final SAMRecord record) {
        return index -> {
            int readPosition = getReadPositionAtReferencePosition(record, index.intValue(), false);
            return readPosition != 0 && fastqToPhred(record.getBaseQualityString().charAt(readPosition)) >= 10;
        };
    }

    private static class Position {
        private final String contigName;
        private final long start;
        private final long end;

        private Position(final String contigName, final long start, final long end) {
            this.contigName = contigName;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Position that = (Position) o;

            return start == that.start && end == that.end && (contigName != null
                    ? contigName.equals(that.contigName)
                    : that.contigName == null);
        }

        @Override
        public int hashCode() {
            int result = contigName != null ? contigName.hashCode() : 0;
            result = 31 * result + (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }
}
