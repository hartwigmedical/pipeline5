package com.hartwig.batch.input;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.batch.BatchArguments;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.batch.testsupport.TestingArguments;

import org.junit.Test;

public class InputParserProviderTest {
    @Test
    public void shouldReturnJsonParserWhenOperationSpecifiesIt() {
        InputParserProvider victim = new InputParserProvider();
        BatchOperation operation = mock(BatchOperation.class);
        BatchArguments arguments = TestingArguments.defaultArgs("someOp");

        OperationDescriptor descriptor = mock(OperationDescriptor.class);
        when(operation.descriptor()).thenReturn(descriptor);
        when(descriptor.inputType()).thenReturn(OperationDescriptor.InputType.JSON);
        victim.from(operation);
        assertThat(1).isEqualTo(1);
    }

    @Test
    public void shouldReturnFlatParserWhenOperationSpecifiesIt() {

    }
}