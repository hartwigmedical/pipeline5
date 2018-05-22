package hmf.sample;

import org.immutables.value.Value;

@Value.Immutable
public interface Sample {

    @Value.Parameter
    String directory();

    @Value.Parameter
    String name();

    static Sample of(String directory, String name) {
        return ImmutableSample.of(directory, name);
    }
}
