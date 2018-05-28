package hmf.testsupport;

import hmf.io.PipelineOutput;
import hmf.sample.FlowCell;
import hmf.sample.Lane;

public class Assertions {

    public static BAMFileAssertion assertThatOutput(Lane lane, PipelineOutput fileType) {
        return new AlignmentFileAssertion(lane, fileType);
    }

    public static BAMFileAssertion assertThatOutput(FlowCell cell) {
        return new DedupedFileAssertion(cell);
    }
}
