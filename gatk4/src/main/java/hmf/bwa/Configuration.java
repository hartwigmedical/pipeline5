package hmf.bwa;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    String getSamplePath();

    String getSampleName();

    String getReferenceFile();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
