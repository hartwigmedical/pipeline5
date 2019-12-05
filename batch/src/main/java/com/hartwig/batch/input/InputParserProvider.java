package com.hartwig.batch.input;

import com.hartwig.batch.BatchArguments;

public class InputParserProvider {
    public static InputParser from(BatchArguments arguments) {
        return arguments.inputFormat().equals(BatchArguments.JSON_INPUT)
                ? new JsonInputParser(arguments.inputFile(), arguments.project())
                : new FlatInputParser(arguments.inputFile(), arguments.project());
    }
}
