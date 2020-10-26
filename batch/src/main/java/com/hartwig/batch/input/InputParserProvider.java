package com.hartwig.batch.input;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;

public class InputParserProvider {
    public InputParser from(BatchOperation batchOperation) {
        return batchOperation.descriptor().inputType() == OperationDescriptor.InputType.JSON ?
                new JsonInputParser() : new FlatInputParser();
    }
}
