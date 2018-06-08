package hmf.testsupport;

import hmf.patient.Sample;

public class Assertions {

    public static AssertionChain assertThatOutput(Sample cell) {
        return new AssertionChain(cell);
    }
}
