package com.hartwig.testsupport;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

public class Assertions {

    public static AssertionChain assertThatOutput(String resultDirectory, OutputType finalFileType, Sample sample) {
        return new AssertionChain(resultDirectory, finalFileType, sample);
    }
}
