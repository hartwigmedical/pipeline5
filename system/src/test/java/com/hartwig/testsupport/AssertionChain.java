package com.hartwig.testsupport;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

public class AssertionChain {
    private final String workingDirectory;
    private final Sample sample;
    private final OutputType finalFileType;
    private final List<BAMFileAssertion> assertions = new ArrayList<>();

    AssertionChain(final String workingDirectory, final OutputType finalFileType, final Sample sample) {
        this.workingDirectory = workingDirectory;
        this.sample = sample;
        this.finalFileType = finalFileType;
    }

    public AssertionChain sorted() {
        assertions.add(new CoordinateSortedBAMFileAssertion(workingDirectory, finalFileType, sample));
        return this;
    }

    public AssertionChain aligned() {
        assertions.add(new AlignmentFileAssertion(workingDirectory, finalFileType, sample));
        return this;
    }

    public AssertionChain duplicatesMarked() {
        assertions.add(new DuplicateMarkedFileAssertion(workingDirectory, finalFileType, sample));
        return this;
    }

    public void isEqualToExpected() {
        assertions.forEach(BAMFileAssertion::isEqualToExpected);
    }
}
