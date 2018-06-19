package com.hartwig.testsupport;

import com.hartwig.patient.Sample;

public class Assertions {

    public static AssertionChain assertThatOutput(Sample cell) {
        return new AssertionChain(cell);
    }
}
