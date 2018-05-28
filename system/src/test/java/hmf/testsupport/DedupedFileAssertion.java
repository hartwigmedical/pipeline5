package hmf.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import hmf.io.PipelineOutput;
import hmf.sample.FlowCell;
import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;

class DedupedFileAssertion extends BAMFileAssertion {

    DedupedFileAssertion(final FlowCell cell) {
        super(PipelineOutput.DEDUPED, cell);
    }

    @Override
    void assertFile(final SamReader expected, final SamReader results) {
        Set<SAMRecord> duplicatesExpected = findDuplicates(expected);
        Set<SAMRecord> duplicatesResults = findDuplicates(results);
        assertThat(duplicatesResults).isEqualTo(duplicatesExpected);
    }

    private Set<SAMRecord> findDuplicates(final SamReader samReaderResults) {
        Set<SAMRecord> duplicates = new HashSet<>();
        for (SAMRecord record : samReaderResults) {
            if (SAMFlag.getFlags(record.getFlags()).contains(SAMFlag.DUPLICATE_READ)) {
                duplicates.add(record);
            }
        }
        return duplicates;
    }
}
