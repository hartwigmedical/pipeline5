package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.Function;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;
import com.hartwig.patient.Sample;
import com.hartwig.patient.io.PatientReader;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalToGoogleStorage implements PatientUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalToGoogleStorage.class);
    private final Storage storage;
    private final String bucket;
    private final Patient patient;

    public LocalToGoogleStorage(final Storage storage, final String bucket, final Patient patient) {
        this.storage = storage;
        this.bucket = bucket;
        this.patient = patient;
    }

    @Override
    public void run() throws IOException {
        LOGGER.info("Uploading patient [{}] into Google Storage", patient.name());
        Bucket bucket = storage.get(this.bucket);
        if (patient.maybeTumor().isPresent()) {
            uploadSample(bucket, patient.reference(), file -> sampleFile(patient, file, PatientReader.TypeSuffix.REFERENCE));
            uploadSample(bucket, patient.tumor(), file -> sampleFile(patient, file, PatientReader.TypeSuffix.TUMOR));
        } else {
            uploadSample(bucket, patient.reference(), file -> singleSampleFile(patient, file));
        }
        LOGGER.info("Upload complete");
    }

    private void uploadSample(final Bucket bucket, final Sample reference, Function<File, String> blobCreator)
            throws FileNotFoundException {
        for (Lane lane : reference.lanes()) {
            File reads = file(lane.readsPath());
            File mates = file(lane.matesPath());
            bucket.create(blobCreator.apply(reads), new FileInputStream(reads));
            bucket.create(blobCreator.apply(mates), new FileInputStream(mates));
        }
    }

    @NotNull
    private static String sampleFile(final Patient patient, final File reads, final PatientReader.TypeSuffix suffix) {
        return String.format("patients/%s/%s%s/%s", patient.name(), patient.name(), suffix.getSuffix(), reads.getName());
    }

    @NotNull
    private static String singleSampleFile(final Patient patient, final File reads) {
        return String.format("patients/%s/%s", patient.name(), reads.getName());
    }

    @NotNull
    private static File file(final String path) {
        return new File(path.replaceAll("file:", ""));
    }
}
