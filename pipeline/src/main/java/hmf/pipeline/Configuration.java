package hmf.pipeline;

import org.immutables.value.Value;

@Value.Immutable
public interface Configuration {

    enum Flavour {
        GATK4,
        ADAM
    }

    @Value.Default
    default Flavour flavour() {
        return Flavour.ADAM;
    }

    String sparkMaster();

    String patientDirectory();

    String patientName();

    String referencePath();

    static ImmutableConfiguration.Builder builder() {
        return ImmutableConfiguration.builder();
    }
}
