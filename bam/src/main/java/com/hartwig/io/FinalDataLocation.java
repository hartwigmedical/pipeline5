package com.hartwig.io;

import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;

public class FinalDataLocation implements DataLocation {

    private final FileSystem fileSystem;
    private final String workingDirectory;

    public FinalDataLocation(FileSystem fileSystem, String workingDirectory) {
        this.fileSystem = fileSystem;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public String uri(Sample sample, String suffix) {
        return String.format("%s%s/%s%s.bam",
                fileSystem.getUri(),
                workingDirectory,
                sample.name(),
                suffix.trim().isEmpty() ? "" : "." + suffix);
    }

    @Override
    public String rootUri() {
        return String.format("%s%s", fileSystem.getUri(), workingDirectory);
    }
}
