package com.hartwig.pipeline.testsupport;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;

class AlignmentFileAssertion extends BAMFileAssertion {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlignmentFileAssertion.class);

    AlignmentFileAssertion(final String resultDirectory, final String sample) {
        super(resultDirectory, sample);
    }

    @Override
    void assertFile(final SamReader expected, final SamReader results) {
        Map<Key, SAMRecord> recordMapExpected = mapOf(expected);
        Map<Key, SAMRecord> recordMapResults = mapOf(results);

        checkRecordCounts(recordMapExpected, recordMapResults);
        compareRecordByRecord(recordMapExpected, recordMapResults);
    }

    private void checkRecordCounts(final Map<Key, SAMRecord> recordMapExpected, final Map<Key, SAMRecord> recordMapResults) {
        assertThat(recordMapExpected.size()).as(
                "Expected and result BAM files have different numbers of alignments. Expected had [%s] and result had [%s]",
                recordMapExpected.size(),
                recordMapResults.size()).isEqualTo(recordMapResults.size());
    }

    private void compareRecordByRecord(final Map<Key, SAMRecord> recordMapExpected, final Map<Key, SAMRecord> recordMapResults) {
        Map<String, Integer> differenceMap = new HashMap<>();
        for (Key key : recordMapExpected.keySet()) {
            SAMRecord samRecordExpected = recordMapExpected.get(key);
            SAMRecord samRecordResult = recordMapResults.get(key);
            if (samRecordResult != null) {
                if (!bothReadsUnmappedOrDuplicate(samRecordExpected, samRecordResult)) {
                    String result = recordEqualsWithoutTags(samRecordExpected, samRecordResult);
                    if (!result.isEmpty()) {
                        differenceMap.putIfAbsent(result, 0);
                        differenceMap.computeIfPresent(result, (s, integer) -> integer + 1);
                    }
                }
            }
        }
        if (!differenceMap.isEmpty()) {
            LOGGER.warn("BAM files were the same size but not exactly equal for sample {}. Difference count by type: {}",
                    getName(),
                    differenceMap);
        }
    }

    private boolean bothReadsUnmappedOrDuplicate(final SAMRecord samRecordExpected, final SAMRecord samRecordResult) {
        return isUnmappedOrDuplicate(samRecordExpected) && isUnmappedOrDuplicate(samRecordResult);
    }

    private boolean isUnmappedOrDuplicate(final SAMRecord record) {
        return hasFlag(record, SAMFlag.READ_UNMAPPED) || hasFlag(record, SAMFlag.MATE_UNMAPPED) || hasFlag(record, SAMFlag.DUPLICATE_READ);
    }

    private boolean hasFlag(final SAMRecord record, final SAMFlag readUnmapped) {
        return SAMFlag.getFlags(record.getFlags()).contains(readUnmapped);
    }

    private static Map<Key, SAMRecord> mapOf(final SamReader samReaderExpected) {
        return stream(samReaderExpected.spliterator(), false).collect(toMap(Key::of, Function.identity(), (key1, key2) -> key1));
    }

    private static String recordEqualsWithoutTags(final SAMRecord record1, final SAMRecord record2) {
        if (record1.getAlignmentStart() != record2.getAlignmentStart()) {
            return "Alignment start";
        }
        if (record1.getFlags() != record2.getFlags()) {
            return "Flags";
        }
        if (record1.getInferredInsertSize() != record2.getInferredInsertSize()) {
            return "Inferred Size";
        }
        if (record1.getMateAlignmentStart() != record2.getMateAlignmentStart()) {
            return "Mate Alignment Start";
        }
        if (safeEquals(record1.getMateReferenceIndex(), record2.getMateReferenceIndex())) {
            return "Mate Reference Index";
        }
        if (safeEquals(record1.getReferenceIndex(), record2.getReferenceIndex())) {
            return "Reference Index";
        }
        if (safeEquals(record1.getReadName(), record2.getReadName())) {
            return "Read Name";
        }
        if (!Arrays.equals(record1.getBaseQualities(), record2.getBaseQualities())) {
            return "Base Qualities";
        }
        if (safeEquals(record1.getCigar(), record2.getCigar())) {
            return "CIGAR";
        }
        if (safeEquals(record1.getMateReferenceName(), record2.getMateReferenceName())) {
            return "Mate Reference Name";
        }
        if (!Arrays.equals(record1.getReadBases(), record2.getReadBases())) {
            return "Read Bases";
        }
        //noinspection RedundantIfStatement
        if (safeEquals(record1.getReferenceName(), record2.getReferenceName())) {
            return "Reference Name";
        }
        return "";
    }

    private static <T> boolean safeEquals(final T attribute1, final T attribute2) {
        return attribute1 != null ? !attribute1.equals(attribute2) : attribute2 != null;
    }

    private static class Key {
        private final String readName;
        private final int flags;

        private Key(SAMRecord record) {
            this.readName = record.getReadName();
            this.flags = record.getFlags();
        }

        static Key of(SAMRecord record) {
            return new Key(record);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Key key = (Key) o;

            return flags == key.flags && (readName != null ? readName.equals(key.readName) : key.readName == null);
        }

        @Override
        public int hashCode() {
            int result = readName != null ? readName.hashCode() : 0;
            result = 31 * result + flags;
            return result;
        }

        @Override
        public String toString() {
            return "Key{" + "readName='" + readName + '\'' + ", flags=" + flags + '}';
        }
    }
}
