package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
interface Configuration {

    String getSampleDirectory();

    String getSampleName();

    String getReferencePath();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
