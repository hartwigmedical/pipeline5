package com.hartwig.batch.input;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.commons.io.FileUtils;

public class FlatInputParser implements InputParser {
    @Override
    public List<InputBundle> parse(final String inputFile, final String billedProject) throws RuntimeException {
        List<InputBundle> fileDescriptors = new ArrayList<>();
        try {
            FileUtils.readLines(new File(inputFile), "UTF-8")
                    .stream()
                    .filter(line -> !line.trim().isEmpty())
                    .collect(Collectors.toSet())
                    .forEach(path -> {
                        InputFileDescriptor built = InputFileDescriptor.builder().inputValue(path)
                                .billedProject(billedProject).name("path").build();
                        fileDescriptors.add(new InputBundle(ImmutableList.of(built)));
                    });
            return fileDescriptors;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to parse inputs file", ioe);
        }
    }
}
