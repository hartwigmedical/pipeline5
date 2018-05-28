package hmf.sample;

import org.immutables.value.Value;

@Value.Immutable
public interface Lane extends HasSample {

    @Value.Parameter
    Sample sample();

    @Value.Parameter
    int index();

    @Override
    default void accept(HasSampleVisitor visitor) {
        visitor.visit(this);
    }

    static Lane of(Sample sample, int index) {
        return ImmutableLane.of(sample, index);
    }
}
