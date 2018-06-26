package com.hartwig.testsupport;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

public class AssertionChain {
    private final Sample sample;
    private final List<BAMFileAssertion> assertions = new ArrayList<>();

    AssertionChain(final Sample sample) {
        this.sample = sample;
    }

    public AssertionChain aligned() {
        assertions.add(new AlignmentFileAssertion(sample, OutputType.DUPLICATE_MARKED));
        return this;
    }

    public AssertionChain duplicatesMarked() {
        assertions.add(new DuplicateMarkedFileAssertion(sample));
        return this;
    }

    public void isEqualToExpected() {
        assertions.forEach(BAMFileAssertion::isEqualToExpected);
    }
}
