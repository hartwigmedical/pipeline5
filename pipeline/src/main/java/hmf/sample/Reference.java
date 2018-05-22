package hmf.sample;

import org.immutables.value.Value;

import hmf.pipeline.Configuration;

@Value.Immutable
public interface Reference {

    @Value.Parameter
    String path();

    static Reference from(Configuration configuration) {
        return ImmutableReference.of(configuration.referencePath());
    }
}
