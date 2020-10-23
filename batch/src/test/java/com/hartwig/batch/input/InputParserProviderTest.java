package com.hartwig.batch.input;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.OperationDescriptor.InputType;

import org.junit.Test;

public class InputParserProviderTest {
    @Test
    public void shouldReturnJsonParserWhenOperationSpecifiesIt() {
        InputParserProvider victim = new InputParserProvider();
        BatchOperation operation = mock(BatchOperation.class);

        OperationDescriptor descriptor = mock(OperationDescriptor.class);
        when(operation.descriptor()).thenReturn(descriptor);
        when(descriptor.inputType()).thenReturn(OperationDescriptor.InputType.JSON);
        InputParser parser = victim.from(operation);
        assertThat(parser.getClass()).isEqualTo(JsonInputParser.class);
    }

    @Test
    public void shouldReturnFlatParserWhenOperationSpecifiesIt() {
        InputParserProvider victim = new InputParserProvider();
        BatchOperation operation = mock(BatchOperation.class);

        OperationDescriptor descriptor = mock(OperationDescriptor.class);
        when(operation.descriptor()).thenReturn(descriptor);
        when(descriptor.inputType()).thenReturn(InputType.FLAT);
        InputParser parser = victim.from(operation);
        assertThat(parser.getClass()).isEqualTo(FlatInputParser.class);
    }
}