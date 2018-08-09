package com.hartwig.pipeline.runtime.patient;

import static com.hartwig.pipeline.runtime.patient.Samples.createPairedEndSample;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

public class ReferenceAndTumorReader implements PatientReader {

    private final FileSystem fileSystem;

    public ReferenceAndTumorReader(final FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Patient read(final Configuration configuration) throws IOException {
        Optional<FileStatus> maybeReferenceDirectory = findDirectoryByConvention(configuration, TypeSuffix.REFERENCE);
        Optional<FileStatus> maybeTumorDirectory = findDirectoryByConvention(configuration, TypeSuffix.TUMOR);
        if (maybeReferenceDirectory.isPresent() && maybeTumorDirectory.isPresent()) {
            return subdirectoriesForReferenceAndTumor(configuration, maybeReferenceDirectory.get(), maybeTumorDirectory.get());
        }
        throw new IllegalArgumentException("Directory structure not as expected. This should be caught in PatientReader");
    }

    private Patient subdirectoriesForReferenceAndTumor(final Configuration configuration, final FileStatus referenceDirectory,
            final FileStatus tumorDirectory) throws IOException {
        return patientOf(configuration,
                createPairedEndSample(fileSystem,
                        referenceDirectory.getPath(),
                        configuration.patient().name(),
                        TypeSuffix.REFERENCE.getSuffix()),
                createPairedEndSample(fileSystem, tumorDirectory.getPath(),
                        configuration.patient().name(), TypeSuffix.TUMOR.getSuffix()));
    }

    private static Patient patientOf(final Configuration configuration, final Sample reference, final Sample tumor) throws IOException {
        return Patient.of(configuration.patient().directory(), configuration.patient().name(), reference, tumor);
    }

    private Optional<FileStatus> findDirectoryByConvention(final Configuration configuration, final TypeSuffix typeSuffix)
            throws IOException {
        return Stream.of(fileSystem.listStatus(new Path(configuration.patient().directory()),
                new GlobFilter(configuration.patient().name() + typeSuffix.getSuffix()))).findFirst();
    }
}
