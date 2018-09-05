package com.hartwig.pipeline.bootstrap;

import static com.hartwig.pipeline.bootstrap.BootstrapOptions.PATIENT_DIRECTORY_FLAG;
import static com.hartwig.pipeline.bootstrap.BootstrapOptions.PATIENT_FLAG;
import static com.hartwig.pipeline.bootstrap.BootstrapOptions.SKIP_UPLOAD_FLAG;
import static com.hartwig.pipeline.bootstrap.BootstrapOptions.options;

import java.io.IOException;
import java.util.function.Function;

import com.hartwig.patient.Patient;
import com.hartwig.patient.io.PatientReader;
import com.hartwig.pipeline.cluster.PatientCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.spark.JarLocation;
import com.hartwig.pipeline.spark.JarUpload;
import com.hartwig.pipeline.spark.Version;
import com.hartwig.pipeline.upload.PatientUpload;

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
    private static final String MAIN_CLASS = "com.hartwig.pipeline.runtime.PipelineRuntime";
    private static final String JAR_LOCATION = "gs://pipeline5-jar/system-local-SNAPSHOT.jar";
    private final Function<Patient, PatientUpload> uploadProvider;
    private final Function<Patient, PatientCluster> clusterProvider;
    private final JarUpload jarUpload;
    private final Version version;
    private final CommandLineParser parser;
    private final FileSystem fileSystem;

    Bootstrap(final Function<Patient, PatientUpload> uploadProvider, final Function<Patient, PatientCluster> clusterProvider,
            final JarUpload jarUpload, final Version version, final CommandLineParser parser, final FileSystem fileSystem) {
        this.uploadProvider = uploadProvider;
        this.clusterProvider = clusterProvider;
        this.jarUpload = jarUpload;
        this.version = version;
        this.parser = parser;
        this.fileSystem = fileSystem;
    }

    void run(String[] args) {
        try {
            CommandLine command = parser.parse(options(), args);
            String patientId = command.getOptionValue(PATIENT_FLAG);
            String patientDirectory = command.getOptionValue(PATIENT_DIRECTORY_FLAG, System.getProperty("user.dir"));
            Patient patient = PatientReader.fromHDFS(fileSystem, patientDirectory, patientId);
            if (!command.hasOption(SKIP_UPLOAD_FLAG)) {
                uploadProvider.apply(patient).run();
            }
            JarLocation location = jarUpload.run(version);
            PatientCluster cluster = clusterProvider.apply(patient);
            cluster.start();
            cluster.submit(SparkJobDefinition.of(MAIN_CLASS, location.uri()));
            cluster.stop();

        } catch (ParseException e) {
            new HelpFormatter().printHelp(COMMAND_NAME, options());
        } catch (IOException e) {
            LOGGER.error("Could not read patient data from filesystem. "
                    + "Check the path exists and is of the format /PATIENT_ID/PATIENT_ID{R|T}", e);
        }
    }
}
