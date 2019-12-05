package com.hartwig.batch.input;

import java.util.List;

public interface InputParser {
    List<List<InputFileDescriptor>> parse() throws RuntimeException;
}
