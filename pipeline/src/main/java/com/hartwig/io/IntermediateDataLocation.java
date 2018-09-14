package com.hartwig.io;

import static java.lang.String.format;

import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;

public class IntermediateDataLocation implements DataLocation {

    private final FileSystem fileSystem;
    private final String workingDirectory;

    public IntermediateDataLocation(final FileSystem fileSystem, final String workingDirectory) {
        this.fileSystem = fileSystem;
        this.workingDirectory = workingDirectory;
    }

    public String uri(OutputType output, Sample sample) {
        return fileSystem.getUri() + path(workingDirectory, output, sample);
    }

    public static String path(String workingDirectory, OutputType output, Sample sample) {
        return format("%s%s", workingDirectory, file(output, sample));
    }

    public static String file(OutputType output, Sample sample) {
        return format("%s_%s.%s", sample.name(), output.toString().toLowerCase(), output.getExtension());
    }
}
