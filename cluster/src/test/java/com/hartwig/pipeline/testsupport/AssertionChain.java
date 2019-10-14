package com.hartwig.pipeline.testsupport;

import java.util.ArrayList;
import java.util.List;

public class AssertionChain {
    private final String workingDirectory;
    private final String sample;
    private final List<BAMFileAssertion> assertions = new ArrayList<>();

    AssertionChain(final String workingDirectory, final String sample) {
        this.workingDirectory = workingDirectory;
        this.sample = sample;
    }

    public AssertionChain sorted() {
        assertions.add(new CoordinateSortedBAMFileAssertion(workingDirectory, sample));
        return this;
    }

    public AssertionChain aligned() {
        assertions.add(new AlignmentFileAssertion(workingDirectory, sample));
        return this;
    }

    public AssertionChain duplicatesMarked() {
        assertions.add(new DuplicateMarkedFileAssertion(workingDirectory, sample));
        return this;
    }

    public void isEqualToExpected() {
        assertions.forEach(BAMFileAssertion::isEqualToExpected);
    }
}
