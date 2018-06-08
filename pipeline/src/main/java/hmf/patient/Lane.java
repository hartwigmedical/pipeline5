package hmf.patient;

import org.immutables.value.Value;

@Value.Immutable
public interface Lane extends FileSystemEntity, Named {

    @Value.Parameter
    @Override
    String directory();

    @Value.Parameter
    @Override
    String name();

    @Value.Parameter
    int index();

    @Override
    default void accept(FileSystemVisitor visitor) {
        visitor.visit(this);
    }

    static Lane of(String directory, String name, int index) {
        return ImmutableLane.of(directory, name, index);
    }
}
