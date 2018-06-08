package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    String sparkMaster();

    String patientDirectory();

    String patientName();

    String referencePath();

    @Value.Default
    default boolean useInterleaved() {
        return false;
    }

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
