package com.hartwig.patient.io;

import static com.hartwig.patient.io.Samples.createPairedEndSample;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

public class ReferenceAndTumorReader implements PatientReader {

    private final FileSystem fileSystem;

    ReferenceAndTumorReader(final FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Patient read(final Path patientPath) throws IOException {
        String patientName = patientPath.getName();
        Optional<FileStatus> maybeReferenceDirectory = findDirectoryByConvention(patientPath, patientName, TypeSuffix.REFERENCE);
        Optional<FileStatus> maybeTumorDirectory = findDirectoryByConvention(patientPath, patientName, TypeSuffix.TUMOR);
        if (maybeReferenceDirectory.isPresent() && maybeTumorDirectory.isPresent()) {
            return subdirectoriesForReferenceAndTumor(patientPath, patientName, maybeReferenceDirectory.get(), maybeTumorDirectory.get());
        }
        throw new IllegalArgumentException("Directory structure not as expected. This should be caught in PatientReader");
    }

    private Patient subdirectoriesForReferenceAndTumor(final Path patientPath, final String patientName,
            final FileStatus referenceDirectory, final FileStatus tumorDirectory) throws IOException {
        return patientOf(patientPath,
                patientName,
                createPairedEndSample(fileSystem, referenceDirectory.getPath(), patientName, TypeSuffix.REFERENCE.getSuffix()),
                createPairedEndSample(fileSystem, tumorDirectory.getPath(), patientName, TypeSuffix.TUMOR.getSuffix()));
    }

    private Patient patientOf(final Path patientPath, final String patientName, final Sample reference, final Sample tumor)
            throws IOException {
        return Patient.of(patientPath.toString(), patientName, reference, tumor);
    }

    private Optional<FileStatus> findDirectoryByConvention(Path patientPath, String patientName, final TypeSuffix typeSuffix)
            throws IOException {
        return Stream.of(fileSystem.listStatus(patientPath, new GlobFilter(patientName + typeSuffix.getSuffix()))).findFirst();
    }
}
