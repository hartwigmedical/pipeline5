package hmf.pipeline;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    String patientDirectory();

    String patientName();

    String referencePath();

    List<String> knownIndelPaths();

    @Value.Default
    default boolean useInterleaved() {
        return false;
    }

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
