package com.hartwig.testsupport;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

public class AssertionChain {
    private final Sample sample;
    private final OutputType finalFileType;
    private final List<BAMFileAssertion> assertions = new ArrayList<>();

    AssertionChain(final Sample sample, final OutputType finalFileType) {
        this.sample = sample;
        this.finalFileType = finalFileType;
    }

    public AssertionChain sorted() {
        assertions.add(new CoordinateSortedBAMFileAssertion(sample, finalFileType));
        return this;
    }

    public AssertionChain aligned() {
        assertions.add(new AlignmentFileAssertion(sample, finalFileType));
        return this;
    }

    public AssertionChain duplicatesMarked() {
        assertions.add(new DuplicateMarkedFileAssertion(sample, finalFileType));
        return this;
    }

    public void isEqualToExpected() {
        assertions.forEach(BAMFileAssertion::isEqualToExpected);
    }
}
