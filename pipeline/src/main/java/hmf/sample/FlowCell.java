package hmf.sample;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface FlowCell {

    List<Lane> lanes();

    static ImmutableFlowCell.Builder builder() {
        return ImmutableFlowCell.builder();
    }
}
