package hmf.testsupport;

import java.util.ArrayList;
import java.util.List;

import hmf.io.PipelineOutput;
import hmf.sample.FlowCell;

public class AssertionChain {
    private final FlowCell flowCell;
    private final List<BAMFileAssertion> assertions = new ArrayList<>();

    AssertionChain(final FlowCell flowCell) {
        this.flowCell = flowCell;
    }

    public AssertionChain aligned() {
        assertions.add(new AlignmentFileAssertion(flowCell, PipelineOutput.DUPLICATE_MARKED));
        return this;
    }

    public AssertionChain duplicatesMarked() {
        assertions.add(new DuplicateMarkedFileAssertion(flowCell));
        return this;
    }

    public void isEqualToExpected() {
        assertions.forEach(BAMFileAssertion::isEqualToExpected);
    }
}
