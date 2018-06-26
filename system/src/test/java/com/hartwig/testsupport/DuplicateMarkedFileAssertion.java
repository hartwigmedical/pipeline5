package com.hartwig.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;

class DuplicateMarkedFileAssertion extends BAMFileAssertion<Sample> {

    DuplicateMarkedFileAssertion(final Sample cell) {
        super(OutputType.DUPLICATE_MARKED, cell);
    }

    @Override
    void assertFile(final SamReader expected, final SamReader results) {
        List<String> duplicatesExpected = findDuplicates(expected);
        List<String> duplicatesResults = findDuplicates(results);
        assertThat(duplicatesResults).containsExactlyInAnyOrder(duplicatesExpected.toArray(new String[duplicatesExpected.size()]));
    }

    private List<String> findDuplicates(final SamReader samReaderResults) {
        List<String> duplicates = new ArrayList<>();
        for (SAMRecord record : samReaderResults) {
            if (SAMFlag.getFlags(record.getFlags()).contains(SAMFlag.DUPLICATE_READ) && !SAMFlag.getFlags(record.getFlags())
                    .contains(SAMFlag.MATE_UNMAPPED)) {
                duplicates.add(record.getReadName());
            }
        }
        return duplicates;
    }
}
