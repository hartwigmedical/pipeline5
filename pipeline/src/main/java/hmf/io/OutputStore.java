package hmf.io;

import hmf.patient.FileSystemEntity;

public interface OutputStore<E extends FileSystemEntity, P> {

    void store(Output<E, P> output);
}
