package com.hartwig.pipeline.cluster.vm;

import org.junit.Test;

import static org.mockito.Mockito.mock;

public class GoogleVirtualMachineTest {
    @Test(expected = IllegalArgumentException.class)
    public void germlineBuilderMethodShouldThrowIllegalArgumentExceptionIfArgumentsAreNull() {
        GoogleVirtualMachine.germline(null);
    }
}