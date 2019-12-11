package com.hartwig.batch.input;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FlatInputParser implements InputParser {
    private String inputFile;
    private String billedProject;

    FlatInputParser(String inputFile, String billedProject) {
        this.inputFile = inputFile;
        this.billedProject = billedProject;
    }

    @Override
    public List<InputBundle> parse() throws RuntimeException {
        List<InputBundle> fileDescriptors = new ArrayList<>();
        try {
            FileUtils.readLines(new File(inputFile), "UTF-8")
                    .stream()
                    .filter(line -> !line.trim().isEmpty())
                    .collect(Collectors.toSet())
                    .forEach(path -> {
                        InputFileDescriptor built = InputFileDescriptor.builder().remoteFilename(path)
                                .billedProject(billedProject).name("path").build();
                        fileDescriptors.add(new InputBundle(ImmutableList.of(built)));
                    });
            return fileDescriptors;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to parse inputs file", ioe);
        }
    }
}
