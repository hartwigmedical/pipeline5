package hmf.pipeline;

import java.io.IOException;

public interface Stage {
    PipelineOutput output();

    void execute() throws IOException;
}
