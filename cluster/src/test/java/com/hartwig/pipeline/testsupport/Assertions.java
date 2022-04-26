package com.hartwig.pipeline.testsupport;

public class Assertions {

    public static AssertionChain assertThatOutput(final String resultDirectory, final String sample) {
        return new AssertionChain(resultDirectory, sample);
    }
}
