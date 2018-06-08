package hmf.io;

import org.immutables.value.Value;

import hmf.patient.FileSystemEntity;

@Value.Immutable
public interface Output<E extends FileSystemEntity, P> {

    @Value.Parameter
    OutputType type();

    @Value.Parameter
    E entity();

    @Value.Parameter
    P payload();

    static <E extends FileSystemEntity, P> Output<E, P> of(OutputType type, E entity, P payload) {
        return ImmutableOutput.of(type, entity, payload);
    }
}
