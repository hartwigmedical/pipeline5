package hmf.sample;

import org.immutables.value.Value;

@Value.Immutable
public interface Lane {

    @Value.Parameter
    Sample sample();

    @Value.Parameter
    int index();

    static Lane of(Sample sample, int index) {
        return ImmutableLane.of(sample, index);
    }
}
