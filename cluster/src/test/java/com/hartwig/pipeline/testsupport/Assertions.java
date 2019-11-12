package com.hartwig.pipeline.testsupport;

public class Assertions {

    public static AssertionChain assertThatOutput(String resultDirectory, String sample) {
        return new AssertionChain(resultDirectory, sample);
    }
}
