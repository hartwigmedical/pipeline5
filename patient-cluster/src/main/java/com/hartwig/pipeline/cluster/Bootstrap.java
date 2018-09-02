package com.hartwig.pipeline.cluster;

import static com.hartwig.pipeline.cluster.BootstrapOptions.PATIENT_DIRECTORY_FLAG;
import static com.hartwig.pipeline.cluster.BootstrapOptions.PATIENT_FLAG;
import static com.hartwig.pipeline.cluster.BootstrapOptions.SKIP_UPLOAD_FLAG;
import static com.hartwig.pipeline.cluster.BootstrapOptions.options;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.patient.io.PatientReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String COMMAND_NAME = "bootstrap-cluster";
    private final String bucket;
    private final Storage storage;
    private final CommandLineParser parser;
    private final FileSystem fileSystem;

    Bootstrap(final String bucket, final Storage storage, final CommandLineParser parser, final FileSystem fileSystem) {
        this.bucket = bucket;
        this.storage = storage;
        this.parser = parser;
        this.fileSystem = fileSystem;
    }

    void run(String[] args) {
        try {
            CommandLine command = parser.parse(options(), args);
            String patientId = command.getOptionValue(PATIENT_FLAG);
            String patientDirectory = command.getOptionValue(PATIENT_DIRECTORY_FLAG, System.getProperty("user.dir"));
            if (!command.hasOption(SKIP_UPLOAD_FLAG)) {
                LOGGER.info("Copying patient [{}] into [Google Cloud]", patientId);
                Patient patient = PatientReader.fromHDFS(fileSystem, patientId, patientDirectory);
                Bucket bucket = storage.get(this.bucket);
                for (Lane lane : patient.reference().lanes()) {
                    File reads = new File(lane.readsPath().replaceAll("file:", ""));
                    File mates = new File(lane.matesPath().replaceAll("file:", ""));
                    bucket.create(patientId + "/" + reads.getName(), new FileInputStream(reads));
                    bucket.create(patientId + "/" + mates.getName(), new FileInputStream(mates));
                }
            }
        } catch (ParseException e) {
            new HelpFormatter().printHelp(COMMAND_NAME, options());
        } catch (IOException e) {
            LOGGER.error("Could not read patient data from filesystem. "
                    + "Check the path exists and is of the format /PATIENT_ID/PATIENT_ID{R|T}", e);
        }
    }
}
