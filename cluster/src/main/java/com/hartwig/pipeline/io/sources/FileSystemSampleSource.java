package com.hartwig.pipeline.io.sources;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;

import com.hartwig.patient.Sample;
import com.hartwig.patient.input.PatientReader;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;

public class FileSystemSampleSource implements SampleSource {

    private final FileSystem fileSystem;
    private final String patientDirectory;

    public FileSystemSampleSource(final FileSystem fileSystem, final String patientDirectory) {
        this.fileSystem = fileSystem;
        this.patientDirectory = patientDirectory;
    }

    @Override
    public SampleData sample(final Arguments arguments) {
        try {
            Sample sample = PatientReader.fromHDFS(fileSystem, patientDirectory, arguments.sampleId()).reference();
            long size = sample.lanes()
                    .stream().flatMap(lane -> Stream.of(lane.firstOfPairPath(), lane.secondOfPairPath()))
                    .map(toFileStatus())
                    .mapToLong(FileStatus::getLen)
                    .sum();
            return SampleData.of(sample, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private Function<String, FileStatus> toFileStatus() {
        return path -> {
            try {
                return fileSystem.getFileStatus(new Path(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
