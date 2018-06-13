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
    String readsFile();

    @Value.Parameter
    String matesFile();

    @Override
    default void accept(FileSystemVisitor visitor) {
        visitor.visit(this);
    }

    static Lane of(String directory, String name, String readsFile, String matesFile) {
        return ImmutableLane.of(directory, name, readsFile, matesFile);
    }

    static ImmutableLane.Builder builder() {
        return ImmutableLane.builder();
    }
}
