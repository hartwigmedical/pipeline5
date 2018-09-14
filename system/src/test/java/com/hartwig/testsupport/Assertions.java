package com.hartwig.testsupport;

import com.hartwig.patient.Sample;

public class Assertions {

    public static AssertionChain assertThatOutput(String resultDirectory, Sample sample) {
        return new AssertionChain(resultDirectory, sample);
    }
}
