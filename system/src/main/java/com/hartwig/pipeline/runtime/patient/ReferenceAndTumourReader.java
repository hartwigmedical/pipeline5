package com.hartwig.pipeline.runtime.patient;

import static com.hartwig.pipeline.runtime.patient.Samples.createPairedEndSample;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.runtime.configuration.Configuration;

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
                createPairedEndSample(referenceDirectory, configuration.patient().name(), TypeSuffix.REFERENCE.getSuffix()),
                createPairedEndSample(tumourDirectory, configuration.patient().name(), TypeSuffix.TUMOUR.getSuffix()));
    }

    private static Patient patientOf(final Configuration configuration, final Sample reference, final Sample tumour) throws IOException {
        return Patient.of(configuration.patient().directory(), configuration.patient().name(), reference, tumour);
    }

    private static Optional<Path> findDirectoryByConvention(final Configuration configuration, final TypeSuffix typeSuffix)
            throws IOException {
        return StreamSupport.stream(Files.newDirectoryStream(Paths.get(configuration.patient().directory()),
                configuration.patient().name() + typeSuffix.getSuffix()).spliterator(), false).findFirst();
    }
}
