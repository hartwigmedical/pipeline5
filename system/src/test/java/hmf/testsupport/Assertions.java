package hmf.testsupport;

import hmf.sample.FlowCell;

public class Assertions {

    public static AssertionChain assertThatOutput(FlowCell cell) {
        return new AssertionChain(cell);
    }
}
