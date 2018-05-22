package hmf.samples;

import static org.assertj.core.api.Assertions.assertThat;

import static hmf.pipeline.ImmutableConfiguration.copyOf;
import static hmf.testsupport.TestSamples.CONFIGURATION;

import org.junit.Test;

import hmf.sample.Lane;
import hmf.sample.RawSequencingOutput;
import hmf.sample.Sample;

public class RawSequencingOutputTest {

    @Test
    public void createOutputFromTwoPairedReadFiles() {
        assertThat(RawSequencingOutput.from(CONFIGURATION).sampled().lanes()).hasSize(2)
                .containsOnly(Lane.of(Sample.of(CONFIGURATION.sampleDirectory(), CONFIGURATION.sampleName()), 1),
                        Lane.of(Sample.of(CONFIGURATION.sampleDirectory(), CONFIGURATION.sampleName()), 2));
    }

    @Test
    public void createOutputFromInterleavedPairedReadFiles() {
        assertThat(RawSequencingOutput.from(copyOf(CONFIGURATION).withUseInterleaved(true))
                .sampled()
                .lanes()).containsOnly(Lane.of(Sample.of(CONFIGURATION.sampleDirectory(), CONFIGURATION.sampleName()), 1),
                Lane.of(Sample.of(CONFIGURATION.sampleDirectory(), CONFIGURATION.sampleName()), 2));
    }
}