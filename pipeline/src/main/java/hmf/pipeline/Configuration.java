package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    String sampleDirectory();

    String sampleName();

    String referencePath();

    @Value.Default
    default boolean useInterleaved() {
        return false;
    }

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
