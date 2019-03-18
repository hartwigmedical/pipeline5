package com.hartwig.testsupport;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import java.util.Map;
import java.util.function.Function;

import com.hartwig.patient.Sample;

import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;

class BaseQualityRecalibratedAssertion extends BAMFileAssertion {

    private static final double LIMIT = 0.5;

    BaseQualityRecalibratedAssertion(final String workingDirectory, final Sample sample) {
        super(workingDirectory, "recalibrated", sample);
    }

    @Override
    void assertFile(final SamReader expected, final SamReader results) {
        Map<RecordMapKey, SAMRecord> expectedByReadName = mapOf(expected);
        Map<RecordMapKey, SAMRecord> resultsByReadName = mapOf(results);
        for (RecordMapKey readKey : resultsByReadName.keySet()) {
            SAMRecord resultRecord = resultsByReadName.get(readKey);
            if (resultRecord.getOriginalBaseQualities() != null) {
                SAMRecord expectedRecord = expectedByReadName.get(readKey);
                if (expectedRecord == null) {
                    fail(String.format("Read [%s] was present in the recalibrated BAM but not found in expected BAM. "
                            + "This may mean you need to update the expected BAM to reflect other algorithmic changes in the pipeline, "
                            + "for instance mapping quality increased somehow.", readKey.readName));
                }
                double averageQualityExpected = averageBaseQuality(expectedRecord);
                double averageQualityResult = averageBaseQuality(resultRecord);
                double averageDifference = Math.abs(averageQualityExpected - averageQualityResult) / averageQualityExpected;
                assertThat(averageDifference).as(
                        "Average difference between expected and actual base qualities of [%s] exceeded limit of [%s]",
                        averageDifference,
                        LIMIT).isLessThan(LIMIT);
            } else {
                fail(String.format("Read [%s] was not recalibrated by pipeline 5", readKey.readName));
            }
        }
    }

    private double averageBaseQuality(final SAMRecord expectedRecord) {
        byte[] baseQualities = expectedRecord.getBaseQualities();
        double totalBQ = 0;
        for (byte baseQuality : baseQualities) {
            totalBQ += baseQuality;
        }
        return totalBQ / baseQualities.length;
    }

    private static Map<RecordMapKey, SAMRecord> mapOf(final SamReader samReaderExpected) {
        return stream(samReaderExpected.spliterator(), false).filter(record -> !samFlagContains(record, SAMFlag.READ_UNMAPPED))
                .filter(record -> !record.isSecondaryOrSupplementary())
                .filter(record -> record.getMappingQuality() > 0)
                .filter(record -> !record.getDuplicateReadFlag())
                .collect(toMap(RecordMapKey::of, Function.identity()));
    }

    private static boolean samFlagContains(SAMRecord record, SAMFlag flag) {
        return SAMFlag.getFlags(record.getFlags()).contains(flag);
    }

    private static class RecordMapKey {
        private final boolean firstOfPair;
        private final String readName;

        private RecordMapKey(final boolean firstOfPair, final String readName) {
            this.firstOfPair = firstOfPair;
            this.readName = readName;
        }

        private static RecordMapKey of(SAMRecord record) {
            return new RecordMapKey(record.getFirstOfPairFlag(), record.getReadName());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final RecordMapKey key = (RecordMapKey) o;

            return firstOfPair == key.firstOfPair && (readName != null ? readName.equals(key.readName) : key.readName == null);
        }

        @Override
        public int hashCode() {
            int result = (firstOfPair ? 1 : 0);
            result = 31 * result + (readName != null ? readName.hashCode() : 0);
            return result;
        }
    }
}