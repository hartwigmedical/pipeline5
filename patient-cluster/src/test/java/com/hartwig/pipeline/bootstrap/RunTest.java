package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;

import org.junit.Test;

public class RunTest {

    private static final ImmutableSample SAMPLE = Sample.builder("", "sample").build();
    private static final LocalDateTime NOW = LocalDateTime.of(2018, 9, 19, 15, 5, 0, 0);

    @Test
    public void idConsistsOfSampleNameAndTimestampWhenArgumentEmpty() {
        Run victim = Run.from(SAMPLE, Arguments.defaults(), NOW);
        assertThat(victim.id()).isEqualTo("run-sample-20180919150500000");
    }

    @Test
    public void idCanBeOveriddenFromArguments() {
        Run victim = Run.from(SAMPLE, Arguments.defaultsBuilder().runId("override").build(), NOW);
        assertThat(victim.id()).isEqualTo("run-sample-override");
    }
}