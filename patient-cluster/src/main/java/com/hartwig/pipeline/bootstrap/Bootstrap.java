package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.function.Supplier;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.patient.Patient;
import com.hartwig.patient.io.PatientReader;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.PatientCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.upload.LocalToGoogleStorage;
import com.hartwig.pipeline.upload.PatientUpload;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String MAIN_CLASS = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    private final Supplier<PatientUpload> uploadProvider;
    private final Supplier<PatientCluster> clusterProvider;
    private final JarUpload jarUpload;
    private final FileSystem fileSystem;

    Bootstrap(final Supplier<PatientUpload> uploadProvider, final Supplier<PatientCluster> clusterProvider, final JarUpload jarUpload,
            final FileSystem fileSystem) {
        this.uploadProvider = uploadProvider;
        this.clusterProvider = clusterProvider;
        this.jarUpload = jarUpload;
        this.fileSystem = fileSystem;
    }

    void run(Arguments arguments) {
        try {
            String patientId = arguments.patientId();
            String patientDirectory = arguments.patientDirectory();
            Patient patient = PatientReader.fromHDFS(fileSystem, patientDirectory, patientId);
            PatientUpload upload = uploadProvider.get();
            if (!arguments.skipPatientUpload()) {
                upload.run(patient, arguments);
            }
            JarLocation location = jarUpload.run(arguments);
            PatientCluster cluster = clusterProvider.get();
            cluster.start(patient, arguments);
            cluster.submit(SparkJobDefinition.of(MAIN_CLASS, location.uri()), arguments);
            if (!arguments.skipPatientUpload()) {
                upload.cleanup(patient, arguments);
            }
            cluster.stop(arguments);

        } catch (IOException e) {
            LOGGER.error("Could not read patient data from filesystem. "
                    + "Check the path exists and is of the format /PATIENT_ID/PATIENT_ID{R|T}", e);
        }
    }

    public static void main(String[] args) {
        BootstrapOptions.from(args).ifPresent(arguments -> {
            try {
                final GoogleCredentials credentials =
                        GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
                Storage storage =
                        StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();
                new Bootstrap(() -> new LocalToGoogleStorage(storage),
                        () -> new GoogleDataprocCluster(credentials),
                        new GoogleStorageJarUpload(storage),
                        Hadoop.localFilesystem()).run(arguments);
            } catch (IOException e) {
                LOGGER.error("Unable to run bootstrap. Credential file was missing or not readable.", e);
            }
        });
    }
}
