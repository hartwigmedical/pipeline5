package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    String sampleDirectory();

    String sampleName();

    String referencePath();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
