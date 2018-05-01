package hmf.pipeline;

import hmf.bwa.BwaConfiguration;
import org.junit.Test;

public class PipelineTest {

    @Test
    public void runPipeline() {
        Pipeline victim = new Pipeline(BwaConfiguration.of("test", "test"));
        victim.execute();
    }

}