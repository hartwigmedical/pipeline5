package com.hartwig.patient.input;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.hartwig.patient.Patient;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PatientReader {

    Logger LOGGER = LoggerFactory.getLogger(PatientReader.class);

    enum TypeSuffix {
        REFERENCE("R"),
        TUMOR("T");
        private final String suffix;

        TypeSuffix(final String postfix) {
            this.suffix = postfix;
        }

        public String getSuffix() {
            return suffix;
        }
    }

    Patient read(final Path patientPath) throws IOException;

    static Patient fromHDFS(FileSystem fileSystem, String patientDirectory, String patientName) throws IOException {
        Path patientRootDirectory = new Path(patientDirectory);
        if (patientName.isEmpty()) {
            LOGGER.debug("No patient name given in yaml file, assuming only one patient present in patient directory");
            FileStatus[] subdirectories = fileSystem.listStatus(patientRootDirectory);
            if (subdirectories.length != 1 || !subdirectories[0].isDirectory()) {
                throw new IllegalStateException("If no patient name is given, there can only be a single sub-directory in the patient "
                        + "directory. This subdirectory should be the patient name.");
            }
            patientName = subdirectories[0].getPath().getName();
        }
        Path resolvedPatientPath = patientRootDirectory.suffix("/" + patientName);

        List<FileStatus> referenceAndTumorPaths = Lists.newArrayList(fileSystem.listStatus(resolvedPatientPath,
                path -> path.getName().endsWith(TypeSuffix.TUMOR.getSuffix()) || (path.getName()
                        .endsWith(TypeSuffix.REFERENCE.getSuffix()))))
                .stream()
                .filter(FileStatus::isDirectory)
                .collect(Collectors.toList());

        if (referenceAndTumorPaths.size() == 2) {
            LOGGER.debug("Running in reference and tumor sample patient reader mode");
            return new ReferenceAndTumorReader(fileSystem).read(resolvedPatientPath);
        } else if (referenceAndTumorPaths.isEmpty()) {
            List<FileStatus> subfiles = Stream.of(fileSystem.listStatus(resolvedPatientPath))
                    .collect(Collectors.toList());
            if (subfiles.isEmpty()) {
                throw illegalArgument(format("Patient path [%s] is empty. Check your pipeline.yaml", resolvedPatientPath));
            }
            LOGGER.debug("Running in single sample patient reader mode");
            return new SingleSampleReader(fileSystem).read(resolvedPatientPath);
        }
        throw illegalArgument(format("Unable to determine patient reader mode for directory [%s]. Check your pipeline.yaml, "
                + "Expectation is one directory suffixed with R and another with T", resolvedPatientPath));

    }

    static IllegalArgumentException illegalArgument(final String format) {
        return new IllegalArgumentException(format);
    }
}
