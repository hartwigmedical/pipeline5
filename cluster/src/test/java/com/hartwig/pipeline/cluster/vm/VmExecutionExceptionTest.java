package com.hartwig.pipeline.cluster.vm;

import org.junit.Assert;
import org.junit.Test;

import static com.hartwig.pipeline.cluster.vm.DataFixture.randomStr;
import static org.assertj.core.api.Assertions.assertThat;

public class VmExecutionExceptionTest {
    @Test
    public void shouldSetMessageToConstructorValue() {
        String message = randomStr();
        assertThat(new VmExecutionException(message, new RuntimeException()).getMessage()).isEqualTo(message);
    }

    @Test
    public void shouldSetCauseException() {
        Exception cause = new RuntimeException(randomStr());
        assertThat(new VmExecutionException(randomStr(), cause).getCause()).isEqualTo(cause);
    }
}