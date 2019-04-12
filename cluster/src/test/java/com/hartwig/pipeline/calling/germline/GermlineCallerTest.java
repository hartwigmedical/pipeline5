package com.hartwig.pipeline.calling.germline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.CommandLineOptions;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.storage.StorageProvider;
import org.junit.Assert;
import org.junit.Test;

public class GermlineCallerTest {
    public static final void main(String[] args) throws Exception {
        Arguments arguments = CommandLineOptions.from(args);
        GoogleCredentials credentials = CredentialProvider.from(arguments).get();
        Storage storage = StorageProvider.from(arguments, credentials).get();

        GoogleStorageLocation bam = GoogleStorageLocation.of("gatk-germlne-bucket", "CPCT12345678R.sorted.bam");
        GoogleStorageLocation recalibratedBam = GoogleStorageLocation.of("gatk-germlne-bucket", "CPCT12345678R.recalibrated.bam");
        Sample sample = Sample.builder("dir", "name").build();

        AlignmentOutput bams = AlignmentOutput.of(bam, recalibratedBam, sample);
        GermlineCallerProvider.from(credentials, storage, arguments).get().run(bams);
    }
}