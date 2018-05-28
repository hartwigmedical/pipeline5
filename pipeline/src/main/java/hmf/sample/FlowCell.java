package hmf.sample;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface FlowCell extends HasSample {

    List<Lane> lanes();

    Sample sample();

    @Override
    default void accept(HasSampleVisitor visitor) {
        visitor.visit(this);
    }

    static ImmutableFlowCell.Builder builder() {
        return ImmutableFlowCell.builder();
    }
}
