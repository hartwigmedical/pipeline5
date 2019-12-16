package com.hartwig.batch.input;

import java.util.List;

public interface InputParser {
    List<InputBundle> parse(String inputFile, String billedProject) throws RuntimeException;
}
