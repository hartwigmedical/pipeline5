package hmf.samples;

import static org.assertj.core.api.Assertions.assertThat;

import static hmf.pipeline.ImmutableConfiguration.copyOf;
import static hmf.testsupport.TestPatients.HUNDREDK_READS_HISEQ;

import org.junit.Test;

import hmf.patient.Lane;
import hmf.patient.RawSequencingOutput;
import hmf.pipeline.Configuration;

public class RawSequencingOutputTest {

    private static final Configuration CONFIGURATION = HUNDREDK_READS_HISEQ;

    @Test
    public void createOutputFromTwoPairedReadFiles() {
        assertThat(RawSequencingOutput.from(CONFIGURATION).patient().real().lanes()).hasSize(2)
                .containsOnly(Lane.of(CONFIGURATION.patientDirectory(), CONFIGURATION.patientName(), 1),
                        Lane.of(CONFIGURATION.patientDirectory(), CONFIGURATION.patientName(), 2));
    }

    @Test
    public void createOutputFromInterleavedPairedReadFiles() {
        assertThat(RawSequencingOutput.from(copyOf(CONFIGURATION).withUseInterleaved(true)).patient().real().lanes()).containsOnly(Lane.of(
                CONFIGURATION.patientDirectory(),
                CONFIGURATION.patientName(),
                1), Lane.of(CONFIGURATION.patientDirectory(), CONFIGURATION.patientName(), 2));
    }
}