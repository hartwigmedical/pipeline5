package com.hartwig.pipeline.cluster.vm;

import org.junit.Test;

import static com.hartwig.pipeline.cluster.vm.DataFixture.randomStr;
import static org.assertj.core.api.Assertions.assertThat;

public class VmInitialisationExceptionTest {
    @Test
    public void shouldSetMessageToConstructorValue() {
        String message = randomStr();
        assertThat(new VmInitialisationException(message, new RuntimeException()).getMessage()).isEqualTo(message);
    }

    @Test
    public void shouldSetCauseException() {
        Exception cause = new RuntimeException(randomStr());
        assertThat(new VmInitialisationException(randomStr(), cause).getCause()).isEqualTo(cause);
    }

    @Test
    public void shouldSetMessageToConstructorValueForSingleArgumentVersion() {
        String message = randomStr();
        assertThat(new VmInitialisationException(message).getMessage()).isEqualTo(message);
    }
}