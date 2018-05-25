package hmf.testsupport;

import hmf.pipeline.PipelineOutput;
import hmf.sample.FlowCell;
import hmf.sample.Lane;

public class Assertions {

    public static AlignmentFileAssertion assertThatOutput(Lane lane, PipelineOutput fileType) {
        return new AlignmentFileAssertion(lane, fileType);
    }

    public static DedupedFileAssertion assertThatOutput(FlowCell cell) {
        return new DedupedFileAssertion(cell);
    }
}
