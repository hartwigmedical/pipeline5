package hmf.pipeline;

import java.io.IOException;

import hmf.io.PipelineOutput;

public interface Stage<T> {
    PipelineOutput output();

    void execute(T input) throws IOException;
}
