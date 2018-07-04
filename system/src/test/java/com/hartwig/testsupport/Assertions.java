package com.hartwig.testsupport;

import com.hartwig.io.OutputType;
import com.hartwig.patient.Sample;

public class Assertions {

    public static AssertionChain assertThatOutput(Sample cell, OutputType finalFileType) {
        return new AssertionChain(cell, finalFileType);
    }
}
