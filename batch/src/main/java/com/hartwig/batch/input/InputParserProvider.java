package com.hartwig.batch.input;

import com.hartwig.batch.BatchArguments;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.operations.OperationDescriptor;

public class InputParserProvider {
    public InputParser from(BatchArguments arguments, BatchOperation batchOperation) {
        return batchOperation.descriptor().inputType() == OperationDescriptor.InputType.JSON ?
                new JsonInputParser(arguments.inputFile(), arguments.project()) :
                new FlatInputParser(arguments.inputFile(), arguments.project());
    }
}
