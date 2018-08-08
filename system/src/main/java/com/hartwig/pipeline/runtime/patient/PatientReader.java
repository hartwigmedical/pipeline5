package com.hartwig.pipeline.runtime.patient;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.hartwig.patient.Patient;
import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PatientReader {

    Logger LOGGER = LoggerFactory.getLogger(PatientReader.class);

    enum TypeSuffix {
        REFERENCE("R"),
        TUMOUR("T");
        private final String suffix;

        TypeSuffix(final String postfix) {
            this.suffix = postfix;
        }

        public String getSuffix() {
            return suffix;
        }
    }

    Patient read(Configuration configuration) throws IOException;

    static Patient fromHDFS(FileSystem fileSystem, Configuration configuration) throws IOException {
        Path patientDirectory = new Path(configuration.patient().directory());
        List<FileStatus> referenceAndTumourPaths = Lists.newArrayList(fileSystem.listStatus(patientDirectory,
                path -> path.getName().endsWith(TypeSuffix.TUMOUR.getSuffix()) || (path.getName()
                        .endsWith(TypeSuffix.REFERENCE.getSuffix()))))
                .stream()
                .filter(FileStatus::isDirectory)
                .collect(Collectors.toList());

        if (referenceAndTumourPaths.size() == 2) {
            LOGGER.info("Running in reference and tumour sample patient reader mode");
            return new ReferenceAndTumourReader(fileSystem).read(configuration);
        } else if (referenceAndTumourPaths.isEmpty()) {
            List<FileStatus> subfiles = Stream.of(fileSystem.listStatus(new Path(configuration.patient().directory())))
                    .filter(FileStatus::isFile)
                    .collect(Collectors.toList());
            if (subfiles.isEmpty()) {
                throw illegalArgument(format("Patient directory [%s] is empty. Check your pipeline.yaml",
                        configuration.patient().directory()));
            }
            LOGGER.info("Running in single sample patient reader mode");
            return new SingleSampleReader(fileSystem).read(configuration);
        }
        throw illegalArgument(format("Unable to determine patient reader mode for directory [%s]. Check your pipeline.yaml, "
                + "Expectation is one directory suffixed with R and another with T", configuration.patient().directory()));

    }

    static IllegalArgumentException illegalArgument(final String format) {
        return new IllegalArgumentException(format);
    }
}
