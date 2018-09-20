package com.hartwig.pipeline.bootstrap;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.s3.AmazonS3;
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
import com.hartwig.pipeline.upload.FileStreamProvider;
import com.hartwig.pipeline.upload.GoogleStorageToStream;
import com.hartwig.pipeline.upload.SBPRestApi;
import com.hartwig.pipeline.upload.SBPS3BamSink;
import com.hartwig.pipeline.upload.SBPS3InputStreamProvider;
import com.hartwig.pipeline.upload.SBPSampleReader;
import com.hartwig.pipeline.upload.SampleDownload;
import com.hartwig.pipeline.upload.SampleUpload;
import com.hartwig.pipeline.upload.StreamToGoogleStorage;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);
    private static final String MAIN_CLASS = "com.hartwig.pipeline.runtime.GoogleCloudPipelineRuntime";
    private final Storage storage;
    private final StaticData referenceGenomeData;
    private final StaticData knownIndelData;
    private final SampleSource sampleSource;
    private final SampleDownload sampleDownload;
    private final SampleUpload sampleUpload;
    private final SampleCluster cluster;
    private final JarUpload jarUpload;

    private Bootstrap(final Storage storage, final StaticData referenceGenomeData, final StaticData knownIndelData,
            final SampleSource sampleSource, final SampleDownload sampleDownload, final SampleUpload sampleUpload,
            final SampleCluster cluster, final JarUpload jarUpload) {
        this.storage = storage;
        this.referenceGenomeData = referenceGenomeData;
        this.knownIndelData = knownIndelData;
        this.sampleSource = sampleSource;
        this.sampleDownload = sampleDownload;
        this.sampleUpload = sampleUpload;
        this.cluster = cluster;
        this.jarUpload = jarUpload;
    }

    private void run(Arguments arguments) {
        try {
            Sample sample = sampleSource.sample(arguments);
            RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, sample, arguments);
            referenceGenomeData.copyInto(runtimeBucket);
            knownIndelData.copyInto(runtimeBucket);
            sampleUpload.run(sample, runtimeBucket);
            JarLocation location = jarUpload.run(runtimeBucket, arguments);
            cluster.start(sample, runtimeBucket, arguments);
            cluster.submit(SparkJobDefinition.of(MAIN_CLASS, location.uri()), arguments);
            sampleDownload.run(sample, runtimeBucket);
            if (!arguments.noCleanup()) {
                cluster.stop(arguments);
                runtimeBucket.cleanup();
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

                if (arguments.sbpApiSampleId().isPresent()) {
                    int sbpSampleId = arguments.sbpApiSampleId().get();
                    SBPRestApi sbpRestApi = SBPRestApi.newInstance(arguments);
                    AmazonS3 s3 = S3.newClient(arguments.sblS3Url());
                    new Bootstrap(storage,
                            new StaticData(storage, "reference_genome"),
                            new StaticData(storage, "known_indels"),
                            a -> new SBPSampleReader(sbpRestApi).read(sbpSampleId),
                            new GoogleStorageToStream(SBPS3BamSink.newInstance(s3, sbpRestApi, sbpSampleId)),
                            new StreamToGoogleStorage(SBPS3InputStreamProvider.newInstance(s3)),
                            new GoogleDataprocCluster(credentials),
                            new GoogleStorageJarUpload()).run(arguments);
                } else {
                    new Bootstrap(storage,
                            new StaticData(storage, "reference_genome"),
                            new StaticData(storage, "known_indels"),
                            fromLocalFilesystem(),
                            new GoogleStorageToStream((sample, bucket, stream) -> {
                                String fileName = sample.name() + ".bam";
                                try (FileOutputStream bamFileOut = new FileOutputStream(fileName)) {
                                    IOUtils.copy(stream, bamFileOut);
                                    bamFileOut.close();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                LOGGER.info("Completed download to file [{}]", fileName);
                            }),
                            new StreamToGoogleStorage(FileStreamProvider.newInstance()),
                            new GoogleDataprocCluster(credentials),
                            new GoogleStorageJarUpload()).run(arguments);
                }

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
