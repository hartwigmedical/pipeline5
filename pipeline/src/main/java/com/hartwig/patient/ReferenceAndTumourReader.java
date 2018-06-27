package com.hartwig.patient;

import static com.hartwig.patient.Samples.createPairedEndSample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.hartwig.pipeline.Configuration;

public class ReferenceAndTumourReader implements PatientReader {

    @Override
    public Patient read(final Configuration configuration) throws IOException {
        Optional<Path> maybeReferenceDirectory = findDirectoryByConvention(configuration, TypeSuffix.REFERENCE);
        Optional<Path> maybeTumourDirectory = findDirectoryByConvention(configuration, TypeSuffix.TUMOUR);
        if (maybeReferenceDirectory.isPresent() && maybeTumourDirectory.isPresent()) {
            return subdirectoriesForReferenceAndTumour(configuration, maybeReferenceDirectory.get(), maybeTumourDirectory.get());
        }
        throw new IllegalArgumentException("Directory structure not as expected. This should be caught in PatientReader");
    }

    private static Patient subdirectoriesForReferenceAndTumour(final Configuration configuration, final Path referenceDirectory,
            final Path tumourDirectory) throws IOException {
        return patientOf(configuration,
                createPairedEndSample(referenceDirectory, configuration.patientName(), TypeSuffix.REFERENCE.getSuffix()),
                createPairedEndSample(tumourDirectory, configuration.patientName(), TypeSuffix.TUMOUR.getSuffix()));
    }

    private static Patient patientOf(final Configuration configuration, final Sample reference, final Sample tumour) throws IOException {
        return Patient.of(configuration.patientDirectory(), configuration.patientName(), reference, tumour);
    }

    private static Optional<Path> findDirectoryByConvention(final Configuration configuration, final TypeSuffix typeSuffix)
            throws IOException {
        return StreamSupport.stream(Files.newDirectoryStream(Paths.get(configuration.patientDirectory()),
                configuration.patientName() + typeSuffix.getSuffix()).spliterator(), false).findFirst();
    }
}
