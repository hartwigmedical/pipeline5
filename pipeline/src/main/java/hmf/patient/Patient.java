package hmf.patient;

import org.immutables.value.Value;

@Value.Immutable
public interface Patient extends FileSystemEntity, Named {

    @Value.Parameter
    @Override
    String directory();

    @Value.Parameter
    @Override
    String name();

    @Value.Parameter
    Sample real();

    @Value.Parameter
    Sample tumour();

    @Override
    default void accept(FileSystemVisitor visitor) {
        visitor.visit(this);
    }

    static Patient of(String directory, String name, Sample real, Sample tumour) {
        return ImmutablePatient.of(directory, name, real, tumour);
    }
}
