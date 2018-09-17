package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.patient.Sample;
import com.hartwig.patient.io.PatientReader;
import com.hartwig.pipeline.cluster.GoogleDataprocCluster;
import com.hartwig.pipeline.cluster.GoogleStorageJarUpload;
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.JarUpload;
import com.hartwig.pipeline.cluster.SampleCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.upload.FileStreamSupplier;
import com.hartwig.pipeline.upload.SBPRestApi;
import com.hartwig.pipeline.upload.SBPS3StreamSupplier;
import com.hartwig.pipeline.upload.SBPSampleReader;
import com.hartwig.pipeline.upload.SampleUpload;
import com.hartwig.pipeline.upload.StreamToGoogleStorage;
import com.hartwig.support.hadoop.Hadoop;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String MAIN_CLASS = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    private final SampleSource sampleSource;
    private final Supplier<SampleUpload> uploadProvider;
    private final Supplier<SampleCluster> clusterProvider;
    private final JarUpload jarUpload;

    private Bootstrap(final SampleSource sampleSource, final Supplier<SampleUpload> uploadProvider,
            final Supplier<SampleCluster> clusterProvider, final JarUpload jarUpload) {
        this.sampleSource = sampleSource;
        this.uploadProvider = uploadProvider;
        this.clusterProvider = clusterProvider;
        this.jarUpload = jarUpload;
    }

    private void run(Arguments arguments) {
        try {
            Sample sample = sampleSource.sample(arguments);
            SampleUpload upload = uploadProvider.get();
            if (!arguments.skipPatientUpload()) {
                upload.run(sample, arguments);
            }
            JarLocation location = jarUpload.run(arguments);
            SampleCluster cluster = clusterProvider.get();
            cluster.start(sample, arguments);
            cluster.submit(SparkJobDefinition.of(MAIN_CLASS, location.uri()), arguments);
            if (!arguments.skipPatientUpload()) {
                upload.cleanup(sample, arguments);
            }
            if (!arguments.noClusterDelete()) {
                cluster.stop(arguments);
            }

        } catch (IOException e) {
            LOGGER.error("Could not read patient data from filesystem. "
                    + "Check the path exists and is of the format /PATIENT_ID/PATIENT_ID{R|T}", e);
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Raw arguments [{}]", Stream.of(args).collect(Collectors.joining(", ")));
        BootstrapOptions.from(args).ifPresent(arguments -> {
            try {
                final GoogleCredentials credentials =
                        GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
                Storage storage =
                        StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();

                SampleUpload sampleUpload = arguments.sbpApiSampleId().<SampleUpload>map(sampleId -> new StreamToGoogleStorage(storage,
                        SBPS3StreamSupplier.newInstance(arguments.sblS3Url()))).orElse(new StreamToGoogleStorage(storage,
                        FileStreamSupplier.newInstance()));

                SampleSource sampleSource =
                        arguments.sbpApiSampleId().<SampleSource>map(sampleId -> a -> new SBPSampleReader(SBPRestApi.newInstance(a)).read(
                                sampleId)).orElse(fromLocalFilesystem());

                new Bootstrap(sampleSource,
                        () -> sampleUpload,
                        () -> new GoogleDataprocCluster(credentials),
                        new GoogleStorageJarUpload(storage)).run(arguments);
            } catch (IOException e) {
                LOGGER.error("Unable to run bootstrap. Credential file was missing or not readable.", e);
            }
        });
    }

    @NotNull
    private static SampleSource fromLocalFilesystem() {
        return a -> {
            try {
                return PatientReader.fromHDFS(Hadoop.localFilesystem(), a.patientDirectory(), a.patientId()).reference();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
