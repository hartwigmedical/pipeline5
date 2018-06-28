package com.hartwig.pipeline.runtime.patient;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;

import com.hartwig.patient.Patient;
import com.hartwig.pipeline.runtime.configuration.Configuration;

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

    static Patient from(Configuration configuration) throws IOException {
        File patientDirectory = new File(configuration.patient().directory());
        File[] subdirectories =
                patientDirectory.listFiles(pathname -> pathname.isDirectory() && (pathname.getName().endsWith(TypeSuffix.TUMOUR.getSuffix())
                        || (pathname.getName().endsWith(TypeSuffix.REFERENCE.getSuffix()))));
        if (subdirectories == null) {
            throw illegalArgument(format("Patient directory [%s] is not a directory. Check your pipeline2.yaml",
                    configuration.patient().directory()));
        }
        if (subdirectories.length == 2) {
            LOGGER.info("Running in reference and tumour sample patient reader mode");
            return new ReferenceAndTumourReader().read(configuration);
        } else if (subdirectories.length == 0) {
            File[] allFiles = patientDirectory.listFiles(File::isFile);
            if (allFiles != null && allFiles.length == 0) {
                throw illegalArgument(format("Patient directory [%s] is empty. Check your pipeline2.yaml",
                        configuration.patient().directory()));
            }
            LOGGER.info("Running in single sample patient reader mode");
            return new SingleSampleReader().read(configuration);
        }
        throw illegalArgument(format("Unable to determine patient reader mode for directory [%s]. Check your pipeline2.yaml, "
                + "Expectation is one directory suffixed with R and another with T", configuration.patient().directory()));

    }

    static IllegalArgumentException illegalArgument(final String format) {
        return new IllegalArgumentException(format);
    }
}
