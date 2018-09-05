package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Patient;

import org.jetbrains.annotations.NotNull;

public class LocalToGoogleStorage implements Upload {

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
        Bucket bucket = storage.get(this.bucket);
        for (Lane lane : patient.reference().lanes()) {
            File reads = file(lane.readsPath());
            File mates = file(lane.matesPath());
            bucket.create(cloudFile(patient, reads), new FileInputStream(reads));
            bucket.create(cloudFile(patient, mates), new FileInputStream(mates));
        }
    }

    @NotNull
    private static String cloudFile(final Patient patient, final File reads) {
        return patient.name() + "/" + reads.getName();
    }

    @NotNull
    private static File file(final String path) {
        return new File(path.replaceAll("file:", ""));
    }
}
