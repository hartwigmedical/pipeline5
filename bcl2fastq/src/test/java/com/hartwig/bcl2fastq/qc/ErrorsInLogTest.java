package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class ErrorsInLogTest {

    private static final ImmutableStats STATS = ImmutableStats.builder().flowcell("test").build();
    private ErrorsInLog victim;

    @Before
    public void setUp() {
        victim = new ErrorsInLog();
    }

    @Test
    public void passesWhenNoErrorsInLog() {
        assertThat(victim.apply(STATS, "blah with 0 errors and blah").pass()).isTrue();
    }

    @Test
    public void failsWhenErrorsInLog() {
        assertThat(victim.apply(STATS, "error").pass()).isFalse();
    }
}