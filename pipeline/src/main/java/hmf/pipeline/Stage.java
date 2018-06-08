package hmf.pipeline;

import java.io.IOException;

import hmf.io.Output;
import hmf.patient.FileSystemEntity;

public interface Stage<T extends FileSystemEntity, P> {

    Output<T, P> execute(T input) throws IOException;
}
