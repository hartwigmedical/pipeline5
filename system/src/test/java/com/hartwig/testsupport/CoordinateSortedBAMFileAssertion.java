package com.hartwig.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

import htsjdk.samtools.SAMFlag;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;

class CoordinateSortedBAMFileAssertion extends BAMFileAssertion<Sample> {

    CoordinateSortedBAMFileAssertion(final Sample sample, final OutputType outputType) {
        super(outputType, sample);
    }

    @Override
    void assertFile(final SamReader expected, final SamReader results) {
        SAMRecord previous = null;
        for (SAMRecord result : results) {
            if (!SAMFlag.getFlags(result.getFlags()).contains(SAMFlag.READ_UNMAPPED)) {
                if (previous != null) {
                    assertThat(result.getReferenceIndex()).isGreaterThanOrEqualTo(previous.getReferenceIndex());
                    if (result.getReferenceIndex().equals(previous.getReferenceIndex())) {
                        assertThat(result.getAlignmentStart()).isGreaterThanOrEqualTo(previous.getAlignmentStart());
                    }
                }
                previous = result;
            }
        }
    }
}
