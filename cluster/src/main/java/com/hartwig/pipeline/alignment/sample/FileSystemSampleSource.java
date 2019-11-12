package com.hartwig.pipeline.alignment.sample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public class FileSystemSampleSource implements SampleSource {

    private final String patientDirectory;

    public FileSystemSampleSource(final String patientDirectory) {
        this.patientDirectory = patientDirectory;
    }

    @Override
    public Sample sample(final SingleSampleRunMetadata metadata) {
        try {
            try (Stream<Path> files = Files.walk(Paths.get(patientDirectory + "/" + metadata.sampleName()), 1)) {
                List<Lane> lanes = FastqFiles.toLanes(files.filter(file -> !Files.isDirectory(file))
                        .map(Path::toString)
                        .collect(Collectors.toList()), patientDirectory, metadata.sampleName());
                return Sample.builder(patientDirectory, metadata.sampleName()).addAllLanes(lanes).build();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
