package hmf.testsupport;

import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import hmf.pipeline.PipelineOutput;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

public class BamAssertions {

    private static final double READ_MISSING_TOLERANCE = 0.01;

    public static SampleFileAssertion assertThatOutput(String sampleName, PipelineOutput fileType) {
        return new SampleFileAssertion(sampleName, fileType);
    }

    public static class SampleFileAssertion {
        private final String sampleName;
        private final PipelineOutput fileType;

        SampleFileAssertion(final String sampleName, final PipelineOutput fileType) {
            this.sampleName = sampleName;
            this.fileType = fileType;
        }

        public void isEqualToExpected() throws IOException {

            InputStream expected = BamAssertions.class.getResourceAsStream(format("/expected/%s", fileType.file(sampleName)));
            if (expected == null) {
                fail(format("No expected file found for sample [%s] and output [%s]. Check that the sample name is correct and there is a "
                        + "file in /src/test/resources/expected to verify against", sampleName, fileType));
            }

            SamReaderFactory samReaderFactory = SamReaderFactory.make();
            SamReader samReaderExpected = samReaderFactory.open(SamInputResource.of(expected));
            SamReader samReaderResults = samReaderFactory.open(new File(fileType.path(sampleName)));

            Map<Key, SAMRecord> recordMapExpected = mapOf(samReaderExpected);
            Map<Key, SAMRecord> recordMapResults = mapOf(samReaderResults);

            checkRecordCounts(recordMapExpected, recordMapResults);

            List<Key> missingReadsInResults = compareRecordByRecord(recordMapExpected, recordMapResults);

            checkMissingReadsAgainstTolerance(recordMapExpected, missingReadsInResults);
        }

        private void checkRecordCounts(final Map<Key, SAMRecord> recordMapExpected, final Map<Key, SAMRecord> recordMapResults) {
            assertThat(recordMapExpected.size()).as(
                    "Expected and result BAM files have different numbers of alignments. " + "Expected had [%s] and result had [%s]",
                    recordMapExpected.size(),
                    recordMapResults.size()).isEqualTo(recordMapResults.size());
        }

        private void checkMissingReadsAgainstTolerance(final Map<Key, SAMRecord> recordMapExpected, final List<Key> missingReadsInResults) {
            if (((double) missingReadsInResults.size() / (double) recordMapExpected.size()) > READ_MISSING_TOLERANCE) {
                String thresholdAsPercentage = Double.toString(READ_MISSING_TOLERANCE * 100);
                String missingReadsAsString = missingReadsInResults.stream().map(Key::toString).collect(Collectors.joining(", "));
                fail("Missing more than %s%% of reads in the result BAM. Reads missing [%s]", thresholdAsPercentage, missingReadsAsString);
            }
        }

        private List<Key> compareRecordByRecord(final Map<Key, SAMRecord> recordMapExpected, final Map<Key, SAMRecord> recordMapResults) {
            List<Key> missingReadsInResults = new ArrayList<>();
            for (Key key : recordMapExpected.keySet()) {
                SAMRecord samRecordExpected = recordMapExpected.get(key);
                SAMRecord samRecordResult = recordMapResults.get(key);
                if (samRecordResult == null) {
                    missingReadsInResults.add(key);
                } else {
                    assertThat(recordEqualsWithoutTags(samRecordExpected, samRecordResult)).as(
                            "BAM files where not equal for sample %s and output %s " + "for read %s",
                            sampleName,
                            fileType,
                            samRecordExpected.getReadName()).isTrue();
                }
            }
            return missingReadsInResults;
        }

        private static Map<Key, SAMRecord> mapOf(final SamReader samReaderExpected) {
            return stream(samReaderExpected.spliterator(), false).collect(toMap(Key::of, Function.identity()));
        }

        private static boolean recordEqualsWithoutTags(final SAMRecord record1, final SAMRecord record2) {
            if (record1.getAlignmentStart() != record2.getAlignmentStart()) {
                return false;
            }
            if (record1.getFlags() != record2.getFlags()) {
                return false;
            }
            if (record1.getInferredInsertSize() != record2.getInferredInsertSize()) {
                return false;
            }
            if (record1.getMappingQuality() != record2.getMappingQuality()) {
                return false;
            }
            if (record1.getMateAlignmentStart() != record2.getMateAlignmentStart()) {
                return false;
            }
            if (safeEquals(record1.getMateReferenceIndex(), record2.getMateReferenceIndex())) {
                return false;
            }
            if (safeEquals(record1.getReferenceIndex(), record2.getReferenceIndex())) {
                return false;
            }
            if (safeEquals(record1.getReadName(), record2.getReadName())) {
                return false;
            }
            if (!Arrays.equals(record1.getBaseQualities(), record2.getBaseQualities())) {
                return false;
            }
            if (safeEquals(record1.getCigar(), record2.getCigar())) {
                return false;
            }
            if (safeEquals(record1.getMateReferenceName(), record2.getMateReferenceName())) {
                return false;
            }
            if (!Arrays.equals(record1.getReadBases(), record2.getReadBases())) {
                return false;
            }
            //noinspection RedundantIfStatement
            if (safeEquals(record1.getReferenceName(), record2.getReferenceName())) {
                return false;
            }
            return true;
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
}
