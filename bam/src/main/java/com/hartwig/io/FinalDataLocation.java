package com.hartwig.io;

import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileSystem;

public class FinalDataLocation implements DataLocation {

    private final FileSystem fileSystem;
    private final String workingDirectory;

    public FinalDataLocation(final FileSystem fileSystem, final String workingDirectory) {
        this.fileSystem = fileSystem;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public String uri(final Sample sample) {
        return String.format("%s%s/%s.bam", fileSystem.getUri(), workingDirectory, sample.name());
    }

    @Override
    public String rootUri() {
        return String.format("%s%s", fileSystem.getUri(), workingDirectory);
    }
}
