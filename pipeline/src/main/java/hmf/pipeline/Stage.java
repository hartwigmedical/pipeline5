package hmf.pipeline;

import java.io.IOException;

public interface Stage<T> {
    PipelineOutput output();

    void execute(T input) throws IOException;
}
