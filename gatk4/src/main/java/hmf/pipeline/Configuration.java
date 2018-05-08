package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
interface Configuration {

    String getSamplePath();

    String getSampleName();

    String getReferenceFile();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
